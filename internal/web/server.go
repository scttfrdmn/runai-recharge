// Package web serves statements.
//
// The page is a VIEW, not an artifact. One handler, three parameters, and the
// same query backs the HTML, the CSV, and the API. Statement generation is the
// only write in the system; everything here is a read.
package web

import (
	"context"
	"embed"
	"encoding/csv"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"slices"
	"strconv"
	"time"

	"github.com/scttfrdmn/runai-recharge/internal/bill"
)

//go:embed templates/*.html
var tmplFS embed.FS

type Server struct {
	biller *bill.Biller
	recon  *bill.Reconciler
	health *bill.Health
	tmpl   *template.Template

	// PollInterval is the cron cadence poll runs at. A watermark older than
	// StaleAfter (a few intervals) means the poll has stalled -- and the poll is
	// the only irreplaceable component, because Run:ai ages its source data out.
	// /healthz 503s past this threshold so an orchestrator or uptime check pages
	// someone instead of the gap sitting silent for a week.
	PollInterval time.Duration
	StaleAfter   time.Duration

	// UnbilledWindow is the trailing span /metrics reports unbilled hours over.
	UnbilledWindow time.Duration

	// now is injected in tests; nil means time.Now.
	now func() time.Time

	// snapshot reads the health instruments; defaults to s.health.Snapshot but
	// is stubbed in tests so the 503-on-stale decision can be pinned without a
	// database.
	snapshot func(ctx context.Context, now time.Time, window time.Duration) (bill.HealthSnapshot, error)

	// Authz returns the set of group IDs a viewer may see, and whether they are
	// an admin. This is a PREDICATE on the same query, not three code paths --
	// but it has to be here from the start or it gets bolted on badly later.
	//
	//   user  -> own rows only
	//   PI    -> whole group, per-user breakdown
	//   admin -> everything
	//
	// Run:ai's SSO subject is the identity; project_group already carries the
	// membership edge.
	Authz func(r *http.Request) (groups []string, admin bool, err error)

	// FiscalYearStart returns the FY boundary containing t. July 1 by default.
	FiscalYearStart func(t time.Time) time.Time
}

func NewServer(b *bill.Biller, rec *bill.Reconciler, h *bill.Health) (*Server, error) {
	t, err := template.ParseFS(tmplFS, "templates/*.html")
	if err != nil {
		return nil, err
	}
	const defaultInterval = 5 * time.Minute
	s := &Server{
		biller: b,
		recon:  rec,
		health: h,
		tmpl:   t,

		PollInterval: defaultInterval,
		// 3x the interval: one missed poll is jitter, three is a stall. This is
		// the page-someone threshold, so it errs toward not crying wolf.
		StaleAfter:     3 * defaultInterval,
		UnbilledWindow: 45 * 24 * time.Hour, // ~1.5 months, so it spans to close-of-month

		// FAIL CLOSED. Do not default this to admin-sees-all "for now": that
		// serves institution-wide financial data on :8080 to anyone who can
		// reach the port. A stub that denies is a TODO. A stub that allows is
		// an incident.
		Authz:           denyAll,
		FiscalYearStart: julyFirst,
	}
	if h != nil {
		s.snapshot = h.Snapshot
	}
	return s, nil
}

func (s *Server) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

var errNoAuthz = errors.New(
	"no Authz configured: wire Server.Authz to your SSO before serving")

func denyAll(*http.Request) ([]string, bool, error) {
	return nil, false, errNoAuthz
}

func julyFirst(t time.Time) time.Time {
	y := t.Year()
	if t.Month() < time.July {
		y--
	}
	return time.Date(y, time.July, 1, 0, 0, 0, 0, time.UTC)
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /statement/{group}", s.statement)
	mux.HandleFunc("GET /reconciliation", s.reconciliation)

	// /healthz and /metrics are NOT behind Authz: a monitoring scraper or an
	// orchestrator's liveness probe carries no SSO identity, and neither exposes
	// any billing data -- only whether the instruments are green and the
	// aggregate counts. This is the whole point of #8: the tripwires already
	// exist; these two routes are how someone hears them.
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /metrics", s.metrics)
	return mux
}

// healthz 503s when the poll has stalled. The poll is the only irreplaceable
// component -- Run:ai ages its source data out, so a gap is unrecoverable -- and
// a poll that fails for ANY reason (including the pagination tripwire firing)
// never advances the watermark, so staleness is the one check that catches every
// poll failure mode at once. Wire this to your uptime check; a 503 here is a
// page-someone event.
func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	snap, err := s.snapshot(r.Context(), s.clock(), s.UnbilledWindow)
	if err != nil {
		http.Error(w, "health: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if !snap.EverPolled {
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintln(w, "STALE: no poll has ever completed")
		return
	}
	age := s.clock().Sub(snap.LastPollSuccess)
	if age > s.StaleAfter {
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintf(w, "STALE: last successful poll %s ago (threshold %s)\n",
			age.Round(time.Second), s.StaleAfter)
		return
	}
	fmt.Fprintf(w, "OK: last poll %s ago\n", age.Round(time.Second))
}

// metrics emits Prometheus text-format gauges for the three signals. No client
// library: three gauges do not justify a dependency, and the text format is
// stable. Scrape these with whatever you already run.
func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	snap, err := s.snapshot(r.Context(), s.clock(), s.UnbilledWindow)
	if err != nil {
		http.Error(w, "metrics: "+err.Error(), http.StatusInternalServerError)
		return
	}

	var lastSuccess int64 // unix seconds; 0 = never, which alerting reads as infinitely stale
	if snap.EverPolled && !snap.LastPollSuccess.IsZero() {
		lastSuccess = snap.LastPollSuccess.Unix()
	}
	stale := 0
	if !snap.EverPolled || s.clock().Sub(snap.LastPollSuccess) > s.StaleAfter {
		stale = 1
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	fmt.Fprintf(w, "# HELP recharge_poll_last_success_timestamp Unix time of the last poll that completed in full (0 = never).\n")
	fmt.Fprintf(w, "# TYPE recharge_poll_last_success_timestamp gauge\n")
	fmt.Fprintf(w, "recharge_poll_last_success_timestamp %d\n", lastSuccess)
	fmt.Fprintf(w, "# HELP recharge_poll_stale 1 if the last successful poll is older than the staleness threshold.\n")
	fmt.Fprintf(w, "# TYPE recharge_poll_stale gauge\n")
	fmt.Fprintf(w, "recharge_poll_stale %d\n", stale)
	fmt.Fprintf(w, "# HELP recharge_orphan_pods Unresolved pods that could not be placed and are not billed.\n")
	fmt.Fprintf(w, "# TYPE recharge_orphan_pods gauge\n")
	fmt.Fprintf(w, "recharge_orphan_pods %d\n", snap.OrphanPods)
	fmt.Fprintf(w, "# HELP recharge_unbilled_gpu_hours GPU-hours in the ledger that joined no rate over the trailing window.\n")
	fmt.Fprintf(w, "# TYPE recharge_unbilled_gpu_hours gauge\n")
	fmt.Fprintf(w, "recharge_unbilled_gpu_hours %g\n", snap.UnbilledGPUHours)
	fmt.Fprintf(w, "# HELP recharge_unbilled_gaps Distinct (cluster,pool,model) configuration gaps producing unbilled hours.\n")
	fmt.Fprintf(w, "# TYPE recharge_unbilled_gaps gauge\n")
	fmt.Fprintf(w, "recharge_unbilled_gaps %d\n", snap.UnbilledGaps)
}

func (s *Server) statement(w http.ResponseWriter, r *http.Request) {
	group := r.PathValue("group")

	allowed, admin, err := s.Authz(r)
	if err != nil {
		http.Error(w, "not authorized", http.StatusForbidden)
		return
	}
	if !admin && !slices.Contains(allowed, group) {
		// Don't leak the existence of other groups.
		http.NotFound(w, r)
		return
	}

	from, to, label, err := parsePeriod(r.URL.Query().Get("period"))
	if err != nil {
		http.Error(w, "Unknown period. Use 2026-Q3 or 2026-04.", http.StatusBadRequest)
		return
	}

	// ?class=aws-burst scopes the statement to one set of node pools sharing a
	// cost basis. Omitted = every class the group touched, on one page.
	class := r.URL.Query().Get("class")

	st, err := s.biller.Render(r.Context(), group, class, from, to, s.FiscalYearStart(from))
	if err != nil {
		http.Error(w, "Could not build the statement.", http.StatusInternalServerError)
		return
	}

	if r.URL.Query().Get("format") == "csv" {
		s.csv(w, st, group, label)
		return
	}

	if err := s.tmpl.ExecuteTemplate(w, "statement.html", view(st, label)); err != nil {
		return
	}
}

func (s *Server) csv(w http.ResponseWriter, st *bill.Statement, group, label string) {
	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition",
		fmt.Sprintf(`attachment; filename="recharge-%s-%s.csv"`, group, label))

	c := csv.NewWriter(w)
	defer c.Flush()

	_ = c.Write([]string{"date", "user", "workload", "type", "description",
		"fund_code", "rate_class", "cluster_pool", "gpu_model", "gpu_alloc",
		"gpu_seconds", "rate_usd_per_gpu_hour", "amount_usd", "running_usd",
		"gpu_util_pct"})

	for _, l := range st.Lines {
		util := ""
		if l.UtilMean != nil {
			util = strconv.FormatFloat(*l.UtilMean, 'f', 1, 64)
		}
		_ = c.Write([]string{
			l.Date.UTC().Format(time.RFC3339),
			l.Submitter, l.Workload, l.Type, l.Description, l.FundCode,
			l.Class, l.NodePool, l.GPUModel,
			strconv.FormatFloat(l.GPUAlloc, 'f', -1, 64),
			strconv.FormatFloat(l.GPUSeconds, 'f', 0, 64),
			strconv.FormatFloat(l.Rate, 'f', 4, 64),
			strconv.FormatFloat(l.Amount, 'f', 2, 64),
			strconv.FormatFloat(l.Running, 'f', 2, 64),
			util,
		})
	}
}
