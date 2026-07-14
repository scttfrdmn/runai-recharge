// Package web serves statements.
//
// The page is a VIEW, not an artifact. One handler, three parameters, and the
// same query backs the HTML, the CSV, and the API. Statement generation is the
// only write in the system; everything here is a read.
package web

import (
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
	tmpl   *template.Template

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

func NewServer(b *bill.Biller, rec *bill.Reconciler) (*Server, error) {
	t, err := template.ParseFS(tmplFS, "templates/*.html")
	if err != nil {
		return nil, err
	}
	return &Server{
		biller: b,
		recon:  rec,
		tmpl:   t,

		// FAIL CLOSED. Do not default this to admin-sees-all "for now": that
		// serves institution-wide financial data on :8080 to anyone who can
		// reach the port. A stub that denies is a TODO. A stub that allows is
		// an incident.
		Authz:           denyAll,
		FiscalYearStart: julyFirst,
	}, nil
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
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return mux
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
