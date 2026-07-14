// recharge: GPU recharge for a Run:ai cluster.
//
//	recharge poll         # cron, ~5min. the only thing that must not break.
//	recharge bill         # close a period. the only write in the billing path.
//	recharge serve        # read-only view over the ledger.
//	recharge usage        # GPU-hours and dollars by cost basis.
//	recharge verify-alloc # DO THIS FIRST. see below.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/scttfrdmn/runai-recharge/internal/bill"
	"github.com/scttfrdmn/runai-recharge/internal/ledger"
	"github.com/scttfrdmn/runai-recharge/internal/runai"
	"github.com/scttfrdmn/runai-recharge/internal/web"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	if len(os.Args) < 2 {
		usage()
	}

	ctx := context.Background()

	if os.Args[1] == "demo" || os.Args[1] == "demo-recon" {
		render := web.RenderDemo
		if os.Args[1] == "demo-recon" {
			render = web.RenderReconDemo
		}
		if err := render(os.Stdout); err != nil {
			log.Error(os.Args[1], "err", err)
			os.Exit(1)
		}
		return
	}

	db, err := pgxpool.New(ctx, env("RECHARGE_DSN", "postgres:///recharge"))
	if err != nil {
		log.Error("database", "err", err)
		os.Exit(1)
	}
	defer db.Close()

	rc := runai.New(
		env("RUNAI_URL", ""),
		env("RUNAI_APP_ID", ""),
		env("RUNAI_APP_SECRET", ""),
	)

	switch os.Args[1] {
	case "poll":
		err = poll(ctx, log, rc, ledger.NewStore(db), ledger.NewNodeCache(db))
	case "bill":
		err = doBill(ctx, log, bill.NewBiller(db), os.Args[2:])
	case "usage":
		err = doUsage(ctx, bill.NewBiller(db), os.Args[2:])
	case "reconcile":
		err = doReconcile(ctx, bill.NewReconciler(db), os.Args[2:])
	case "serve":
		err = serve(ctx, log, bill.NewBiller(db), bill.NewReconciler(db))
	case "verify-alloc":
		err = verifyAlloc(ctx, rc, os.Args[2:])
	default:
		usage()
	}

	if err != nil {
		log.Error(os.Args[1], "err", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: recharge {poll|bill|usage|reconcile|serve|verify-alloc|demo|demo-recon}")
	os.Exit(2)
}

// ---------------------------------------------------------------------------
// poll
// ---------------------------------------------------------------------------

// poll is the only component whose failure is unrecoverable, because the source
// data ages out of Run:ai. Everything else can be re-derived from the ledger.
//
// It is idempotent: safe to crash, safe to double-run, safe to replay a window.
func poll(ctx context.Context, log *slog.Logger, rc *runai.Client, st *ledger.Store, nc *ledger.NodeCache) error {
	now := time.Now().UTC()

	// Node cache FIRST. It is the only thing that knows where a GPU is and what
	// model it is. In AWS, nodes scale to zero and return with new names; if we
	// ingest a workload before refreshing, its pods resolve to nothing.
	nodes, err := rc.ListNodes(ctx)
	if err != nil {
		return fmt.Errorf("list nodes: %w", err)
	}
	if err := nc.Refresh(ctx, nodes); err != nil {
		return fmt.Errorf("refresh node cache: %w", err)
	}

	// Record what we HAD. Without this, "idle" is unanswerable, and an idle
	// cloud pool bills nobody while nobody notices.
	if err := nc.RecordCapacity(ctx, nodes, now, pollInterval); err != nil {
		return fmt.Errorf("record capacity: %w", err)
	}

	wm, err := st.Watermark(ctx)
	if err != nil {
		return err
	}
	if wm.IsZero() {
		// Cold start. Bound it — do not ask Run:ai for all of history.
		wm = now.Add(-7 * 24 * time.Hour)
		log.Info("cold start", "since", wm)
	}

	// Overlap the window. Cheap, and it closes the gap where a workload
	// completes between the last poll's cutoff and this one's.
	since := wm.Add(-15 * time.Minute)

	ws, err := rc.ListWorkloads(ctx, since, now)
	if err != nil {
		return err
	}

	var ingested, skipped, orphaned int
	for _, w := range ws {
		if w.Spec.GPUAlloc() == 0 || w.StartedAt == nil {
			skipped++
			continue // never started, or CPU-only. not billable.
		}

		end := now
		if w.CompletedAt != nil {
			end = *w.CompletedAt
		}

		// Placement is a property of the PODS, resolved from the NODE.
		// w.Spec.NodePools is a REQUEST -- what the user asked for, not where
		// the pods landed. Billing off it bills off an intention.
		pods, err := rc.WorkloadPods(ctx, w.ID)
		if err != nil {
			return fmt.Errorf("pods for %s: %w", w.ID, err)
		}

		places, unresolved, err := nc.Resolve(ctx, pods, *w.StartedAt, w.Spec.GPUAlloc())
		if err != nil {
			return err
		}

		// A pod on a node we have never seen. Do NOT guess: guessing placement
		// is the one thing this system must never do. It is the difference
		// between an auditable statement and a plausible one.
		if len(unresolved) > 0 {
			orphaned += len(unresolved)
			log.Warn("unresolved placement -- NOT BILLED",
				"workload", w.ID, "name", w.Name, "pods", len(unresolved))
		}

		// Utilization is decoration. A missing series must never block billing.
		util, err := rc.UtilizationByHour(ctx, w.ID, *w.StartedAt, end)
		if err != nil {
			log.Warn("utilization unavailable", "workload", w.ID, "err", err)
			util = nil
		}

		rec := ledger.WorkloadRecord{
			ID:          w.ID,
			Submitter:   w.SubmittedBy,
			Project:     w.ProjectName,
			Department:  w.DepartmentName,
			ClusterID:   w.ClusterID,
			Name:        w.Name,
			Type:        w.Type,
			Image:       w.Spec.Image,
			Annotations: w.Annotations,
			StartedAt:   *w.StartedAt,
			CompletedAt: w.CompletedAt,
			Phase:       w.Phase,
		}

		// Orphans are persisted, not just logged. They land on the
		// reconciliation statement where an operator has to look at them.
		if len(unresolved) > 0 {
			if err := st.RecordOrphans(ctx, rec, unresolved); err != nil {
				return err
			}
		} else {
			// A node that came back in the cache resolves an earlier orphan.
			if err := st.ResolveOrphans(ctx, w.ID); err != nil {
				return err
			}
		}

		if len(places) == 0 {
			continue
		}

		if err := st.Ingest(ctx, rec, places, util, now); err != nil {
			return fmt.Errorf("ingest %s: %w", w.ID, err)
		}
		ingested++
	}

	log.Info("poll", "ingested", ingested, "skipped", skipped,
		"orphaned_pods", orphaned, "nodes", len(nodes), "since", since)

	// Orphaned pods mean the ledger is incomplete for this window. That is a
	// billing gap, not a warning to scroll past -- and it is now durable, on
	// the reconciliation statement, rather than living in stderr.
	if orphaned > 0 {
		log.Error("pods could not be placed; see the reconciliation statement",
			"orphaned_pods", orphaned)
	}

	// Advance the watermark ONLY now, after every workload in the window has
	// been ingested without error. Any early return above leaves it where it
	// was, so the next run re-reads the whole window -- which is free, because
	// every write in the poll path is idempotent.
	//
	// The old watermark was max(last_polled_at) across the workload table. A
	// poll that ingested 1 of 500 and then crashed advanced it past the 499 it
	// never wrote, and they fell outside every subsequent window forever.
	return st.CommitWatermark(ctx, now)
}

// pollInterval must match your cron. It is what capacity presence is credited
// in: poll every 5 min and each poll credits a node with 5/60 of the hour.
var pollInterval = 5 * time.Minute

// ---------------------------------------------------------------------------
// bill
// ---------------------------------------------------------------------------

func doBill(ctx context.Context, log *slog.Logger, b *bill.Biller, args []string) error {
	fs := flag.NewFlagSet("bill", flag.ExitOnError)
	group := fs.String("group", "", "billing group id")
	class := fs.String("class", "", "rate class to scope to; empty = all capacity the group touched")
	from := fs.String("from", "", "period start, YYYY-MM-DD")
	to := fs.String("to", "", "period end (exclusive), YYYY-MM-DD")
	_ = fs.Parse(args)

	f, err := time.Parse("2006-01-02", *from)
	if err != nil {
		return err
	}
	t, err := time.Parse("2006-01-02", *to)
	if err != nil {
		return err
	}

	sid, err := b.Close(ctx, *group, *class, f, t)
	if err != nil {
		return err
	}
	log.Info("closed", "statement", sid, "group", *group, "class", *class)
	return nil
}

// ---------------------------------------------------------------------------
// usage
// ---------------------------------------------------------------------------

func doUsage(ctx context.Context, b *bill.Biller, args []string) error {
	fs := flag.NewFlagSet("usage", flag.ExitOnError)
	from := fs.String("from", "", "YYYY-MM-DD")
	to := fs.String("to", "", "YYYY-MM-DD")
	_ = fs.Parse(args)

	f, err := time.Parse("2006-01-02", *from)
	if err != nil {
		return err
	}
	t, err := time.Parse("2006-01-02", *to)
	if err != nil {
		return err
	}

	us, err := b.Usage(ctx, f, t)
	if err != nil {
		return err
	}

	fmt.Printf("%-14s %-24s %-16s %12s %14s\n",
		"CLASS", "CAPACITY", "GPU MODEL", "GPU-HOURS", "BILLED")
	var total float64
	for _, u := range us {
		fmt.Printf("%-14s %-24s %-16s %12.1f %14.2f\n",
			u.Class, u.ClassName, u.GPUModel, u.GPUHours, u.Billed)
		total += u.Billed
	}
	fmt.Printf("%-14s %-24s %-16s %12s %14.2f\n", "", "", "", "TOTAL", total)

	// Reconciliation. Ledger rows that failed to join a rate are dropped
	// silently by every other query in the system. They are usage nobody is
	// paying for and nobody is looking at.
	ub, err := b.Unbilled(ctx, f, t)
	if err != nil {
		return err
	}
	if len(ub) == 0 {
		return nil
	}

	fmt.Println()
	fmt.Println("UNBILLED -- these GPU-hours are in the ledger but on no statement:")
	fmt.Println()
	var lost float64
	for _, u := range ub {
		fmt.Printf("  %s/%s  %s  %.1f GPU-hours\n    %s\n",
			u.ClusterID, u.NodePool, u.GPUModel, u.GPUHours, u.Reason)
		lost += u.GPUHours
	}
	fmt.Printf("\n  %.1f GPU-hours unbilled. Fix pool_class / rate, then re-run.\n", lost)

	return fmt.Errorf("%d unbilled configuration gap(s)", len(ub))
}

// ---------------------------------------------------------------------------
// reconcile — same query as /reconciliation. Exits non-zero if the period
// cannot be closed without leaving money on the floor.
// ---------------------------------------------------------------------------

func doReconcile(ctx context.Context, rec *bill.Reconciler, args []string) error {
	fs := flag.NewFlagSet("reconcile", flag.ExitOnError)
	from := fs.String("from", "", "YYYY-MM-DD")
	to := fs.String("to", "", "YYYY-MM-DD")
	_ = fs.Parse(args)

	f, err := time.Parse("2006-01-02", *from)
	if err != nil {
		return err
	}
	t, err := time.Parse("2006-01-02", *to)
	if err != nil {
		return err
	}

	r, err := rec.Run(ctx, f, t)
	if err != nil {
		return err
	}

	fmt.Printf("  capacity   %10.1f GPU-hours\n", r.TotalCapacityHours)
	fmt.Printf("- idle       %10.1f            (%.0f%%, operational)\n",
		r.TotalIdleHours, pct(r.TotalIdleHours, r.TotalCapacityHours))
	fmt.Printf("= allocated  %10.1f\n", r.TotalAllocatedHours)
	fmt.Printf("- unbilled   %10.1f            (config gap)\n", r.TotalUnbilledHours)
	fmt.Printf("= billed     %10s\n\n", fmt.Sprintf("$%.2f", r.TotalBilled))

	fmt.Printf("%-22s %-28s %-12s %10s %10s %10s %12s\n",
		"COST BASIS", "CLUSTER/POOL", "GPU MODEL",
		"CAPACITY", "ALLOC", "UNBILLED", "BILLED")
	for _, c := range r.Capacities {
		fmt.Printf("%-22s %-28s %-12s %10.1f %10.1f %10.1f %12.2f\n",
			c.ClassName, c.ClusterID+"/"+c.NodePool, c.GPUModel,
			c.CapacityHours, c.AllocatedHours, c.UnbilledHours(), c.Billed)
	}

	if r.Clean() {
		fmt.Println("\nClean. Every GPU-hour in the ledger is on a statement.")
		return nil
	}

	fmt.Printf("\n%d EXCEPTION(S) -- usage that is on no statement:\n\n",
		len(r.Unbilled)+len(r.Orphans))
	for _, u := range r.Unbilled {
		fmt.Printf("  %.1f GPU-hours  %s/%s  %s\n    %s\n",
			u.GPUHours, u.ClusterID, u.NodePool, u.GPUModel, u.Reason)
	}
	for _, o := range r.Orphans {
		fmt.Printf("  unplaceable pod  %s  (%s, %s)\n    node %s was gone before we could resolve it\n",
			o.PodName, o.WorkloadName, o.Submitter, o.NodeName)
	}
	return fmt.Errorf("period is not clean; fix pool_class / rate, then re-run")
}

func pct(a, b float64) float64 {
	if b <= 0 {
		return 0
	}
	return a / b * 100
}

// ---------------------------------------------------------------------------
// serve
// ---------------------------------------------------------------------------

func serve(ctx context.Context, log *slog.Logger, b *bill.Biller, rec *bill.Reconciler) error {
	s, err := web.NewServer(b, rec)
	if err != nil {
		return err
	}

	// Authz FAILS CLOSED by default: every route 404s until you wire this.
	// The predicate is already threaded through the query path; it needs an
	// identity. Run:ai's SSO subject is the identity, and project_group already
	// carries the membership edge.
	//
	//   s.Authz = func(r *http.Request) ([]string, bool, error) { ... }
	//
	// For a single-operator pilot, this is the escape hatch -- and it is
	// deliberately something you have to type:
	if os.Getenv("RECHARGE_INSECURE_ADMIN") == "yes-i-mean-it" {
		log.Warn("AUTHZ DISABLED -- every route is world-readable")
		s.Authz = func(*http.Request) ([]string, bool, error) { return nil, true, nil }
	}

	addr := env("RECHARGE_ADDR", ":8080")
	log.Info("serving", "addr", addr)

	srv := &http.Server{
		Addr:              addr,
		Handler:           s.Routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	return srv.ListenAndServe()
}

// ---------------------------------------------------------------------------
// verify-alloc — RUN THIS BEFORE YOU TRUST A SINGLE NUMBER
// ---------------------------------------------------------------------------

// Run:ai aggregates metrics at the workload level across worker pods. Whether
// GPU_ALLOCATION comes back SUMMED across workers or PER-POD determines whether
// a 4-worker x 8-GPU job bills as 32 GPUs or as 8.
//
// Submit one distributed job of a known shape. Run this. Check the arithmetic.
// This is exactly the kind of thing that produces a statement off by 4x and
// goes unnoticed for two quarters.
func verifyAlloc(ctx context.Context, rc *runai.Client, args []string) error {
	fs := flag.NewFlagSet("verify-alloc", flag.ExitOnError)
	workers := fs.Int("workers", 0, "workers you actually submitted")
	gpus := fs.Int("gpus-per-worker", 0, "GPUs per worker you actually requested")
	_ = fs.Parse(args)

	if *workers == 0 || *gpus == 0 {
		return fmt.Errorf("give -workers and -gpus-per-worker for the job you submitted")
	}

	ws, err := rc.ListWorkloads(ctx, time.Now().Add(-2*time.Hour), time.Now())
	if err != nil {
		return err
	}

	fmt.Printf("submitted: %d workers x %d GPU = %d GPUs total\n\n",
		*workers, *gpus, *workers**gpus)

	for _, w := range ws {
		alloc := w.Spec.GPUAlloc()
		if alloc == 0 {
			continue
		}
		fmt.Printf("%-36s %-12s spec.GPUAlloc=%-8g", w.Name, w.Type, alloc)

		switch {
		case int(alloc) == *workers**gpus:
			fmt.Printf("  SUMMED across workers -> bill as-is\n")
		case int(alloc) == *gpus:
			fmt.Printf("  PER-POD -> MULTIPLY BY WORKER COUNT before billing\n")
		default:
			fmt.Printf("  ?? neither %d nor %d. read the API docs for your version.\n",
				*workers**gpus, *gpus)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
