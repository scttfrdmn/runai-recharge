package main

// End-to-end test of the whole spine: a fake Run:ai control plane -> poll ->
// the ledger -> a statement and a reconciliation, against a REAL Postgres.
//
// The unit tests pin the arithmetic (bucket_test, placement_test) and the guards
// (client_test, main_test). This pins that they compose correctly through the
// SQL -- the part no unit test touches. It exercises the invariants the system
// exists to protect:
//
//   - placement comes from pods, not spec.nodePools
//   - a job preempted across two pools produces rows in BOTH and sums correctly
//   - a pod on an unknown node is an orphan: not billed, on the reconciliation
//   - the poll path is idempotent: a second poll changes nothing
//   - idle and unbilled are distinct, and capacity - allocated = idle
//
// It skips unless RECHARGE_TEST_DSN points at a Postgres it may DROP SCHEMA on.
// CI provides one; `go test ./...` on a laptop without it is a clean skip.

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/scttfrdmn/runai-recharge/internal/bill"
	"github.com/scttfrdmn/runai-recharge/internal/ledger"
	"github.com/scttfrdmn/runai-recharge/internal/runai"
)

const gpuModel = "NVIDIA-H100-80GB-HBM3" // raw nvidia.com/gpu.product; rates key on it exactly

func tp(t time.Time) *time.Time { return &t }
func ip(i int) *int             { return &i }
func fp(f float64) *float64     { return &f }

// fakeNode mirrors the JSON the /nodes endpoint returns. Built by hand because
// runai.Node nests an anonymous struct that is awkward to construct as a literal.
type fakeNode struct {
	Name      string `json:"name"`
	ClusterID string `json:"clusterId"`
	NodePool  string `json:"nodePool"`
	Status    struct {
		GPUInfo struct {
			GPUType  string `json:"gpuType"`
			GPUCount int    `json:"gpuCount"`
		} `json:"gpuInfo"`
	} `json:"status"`
}

func node(name, pool string, count int) fakeNode {
	n := fakeNode{Name: name, ClusterID: "onprem-cluster", NodePool: pool}
	n.Status.GPUInfo.GPUType = gpuModel
	n.Status.GPUInfo.GPUCount = count
	return n
}

// fakeRunai stands in for the Run:ai control plane. All timestamps are anchored
// to `base` so GPU-seconds are exact and the run is deterministic.
func fakeRunai(base time.Time) *httptest.Server {
	nodes := []fakeNode{
		node("onprem-1", "default", 16),
		node("aws-1", "aws", 8),
		// ghost-node is deliberately ABSENT: wl-c's pod lands on it and must
		// become an orphan rather than being guessed onto some other node.
	}

	descr := map[string]string{"recharge.description": "protein fold", "recharge.fund-code": "R01"}
	workload := func(id string, alloc int, end time.Time) runai.Workload {
		return runai.Workload{
			ID: id, Name: id, Type: "Training", Phase: "Completed",
			ClusterID: "onprem-cluster", SubmittedBy: "jchen", ProjectName: "neuro-chen",
			CreatedAt: base, StartedAt: tp(base), CompletedAt: tp(end),
			Spec:        runai.WorkloadSpec{Image: "train:1", GPUDevicesRequest: ip(alloc), NodePools: []string{"default"}},
			Annotations: descr,
		}
	}
	workloads := []runai.Workload{
		workload("wl-a", 8, base.Add(2*time.Hour)),
		workload("wl-b", 1, base.Add(2*time.Hour)),
		workload("wl-c", 4, base.Add(1*time.Hour)),
	}

	pods := map[string][]runai.Pod{
		// One pod, 8 GPU, two full hours on onprem.
		"wl-a": {{Name: "wl-a-0", NodeName: "onprem-1", Phase: "Succeeded",
			StartedAt: tp(base), CompletedAt: tp(base.Add(2 * time.Hour)), RequestedGPUs: fp(8)}},
		// Preempted: hour 1 on onprem, hour 2 on aws. Two placements, one workload.
		"wl-b": {
			{Name: "wl-b-0", NodeName: "onprem-1", Phase: "Succeeded",
				StartedAt: tp(base), CompletedAt: tp(base.Add(time.Hour)), RequestedGPUs: fp(1)},
			{Name: "wl-b-1", NodeName: "aws-1", Phase: "Succeeded",
				StartedAt: tp(base.Add(time.Hour)), CompletedAt: tp(base.Add(2 * time.Hour)), RequestedGPUs: fp(1)},
		},
		// Pod on a node we never saw -> orphan.
		"wl-c": {{Name: "wl-c-0", NodeName: "ghost-node", Phase: "Succeeded",
			StartedAt: tp(base), CompletedAt: tp(base.Add(time.Hour)), RequestedGPUs: fp(4)}},
	}

	writeJSON := func(w http.ResponseWriter, v any) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(v)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/token", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, map[string]any{"accessToken": "test-token", "expiresIn": 3600})
	})
	mux.HandleFunc("GET /api/v1/nodes", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, map[string]any{"nodes": nodes})
	})
	mux.HandleFunc("GET /api/v1/workloads", func(w http.ResponseWriter, _ *http.Request) {
		// Return everything on one short page: in-window for the 7-day cold start,
		// under the page limit, so pagination terminates without tripping.
		writeJSON(w, map[string]any{"workloads": workloads, "nextPageToken": ""})
	})
	mux.HandleFunc("GET /api/v1/workloads/{id}/pods", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"pods": pods[r.PathValue("id")]})
	})
	mux.HandleFunc("GET /api/v1/workloads/{id}/metrics", func(w http.ResponseWriter, _ *http.Request) {
		// Utilization is decoration; an empty series must never affect billing.
		writeJSON(w, map[string]any{"measurements": []any{}})
	})
	return httptest.NewServer(mux)
}

// freshSchema drops and recreates public, then applies the migration. Uses the
// simple protocol because the migration is a multi-statement script.
func freshSchema(ctx context.Context, t *testing.T, dsn string) {
	t.Helper()
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	if _, err := conn.Exec(ctx, "DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;"); err != nil {
		t.Fatalf("reset schema: %v", err)
	}
	schema, err := os.ReadFile("../../migrations/0001_schema.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	if _, err := conn.Exec(ctx, string(schema)); err != nil {
		t.Fatalf("apply migration: %v", err)
	}
}

func TestPollToStatementIntegration(t *testing.T) {
	dsn := os.Getenv("RECHARGE_TEST_DSN")
	if dsn == "" {
		// A laptop with no Postgres skips. CI must NOT: a skip is a silent zero,
		// and a green CI that ran nothing is worse than a red one. If someone
		// breaks the service block and the DSN stops resolving, this is the only
		// thing covering the SQL -- so it fails loudly rather than passing by
		// skipping. Same move as every other tripwire in this codebase.
		if os.Getenv("CI") != "" {
			t.Fatal("RECHARGE_TEST_DSN unset in CI -- the integration test is the " +
				"only coverage for the SQL and the way poll composes through it. " +
				"Refusing to pass by skipping.")
		}
		t.Skip("no RECHARGE_TEST_DSN; set it to a disposable Postgres to run this")
	}
	ctx := context.Background()

	// Anchor on the hour, three hours back, so all work lands in whole PAST hours
	// -> exact GPU-seconds, no open hour, no dependence on wall-clock jitter.
	base := time.Now().UTC().Truncate(time.Hour).Add(-3 * time.Hour)
	cfgFrom := base.Add(-24 * time.Hour) // config effective_from, before any usage
	from := base.Add(-time.Hour)
	to := base.Add(150 * time.Minute) // covers [base, base+2h); excludes the poll's own now-slot

	freshSchema(ctx, t, dsn)

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()

	// Config: two cost bases, on-prem cheaper than AWS burst, same silicon.
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
	exec(`INSERT INTO rate_class VALUES ('onprem','On-premises capacity',NULL),('aws-burst','AWS burst capacity',NULL)`)
	exec(`INSERT INTO pool_class VALUES ('onprem-cluster','default','onprem',$1,NULL),('onprem-cluster','aws','aws-burst',$1,NULL)`, cfgFrom)
	exec(`INSERT INTO rate (class_id,gpu_model,usd_per_gpu_hour,effective_from) VALUES ('onprem',$1,1.05,$2),('aws-burst',$1,2.40,$2)`, gpuModel, cfgFrom)
	exec(`INSERT INTO billing_group VALUES ('chen-lab','Chen Lab','R01','jchen@example.edu')`)
	exec(`INSERT INTO project_group VALUES ('neuro-chen','chen-lab',$1,NULL)`, cfgFrom)

	srv := fakeRunai(base)
	defer srv.Close()
	rc := runai.New(srv.URL, "app", "secret")
	st := ledger.NewStore(pool)
	nc := ledger.NewNodeCache(pool)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	// --- poll ---------------------------------------------------------------
	if err := poll(ctx, log, rc, st, nc); err != nil {
		t.Fatalf("poll: %v", err)
	}

	// Seed capacity across the allocation window. poll only records ONE capacity
	// observation (at real `now`, outside our window); a realistic reconciliation
	// needs presence across the hours the work actually ran.
	nodes, err := rc.ListNodes(ctx)
	if err != nil {
		t.Fatalf("list nodes: %v", err)
	}
	for slot := base; slot.Before(base.Add(2 * time.Hour)); slot = slot.Add(5 * time.Minute) {
		if err := nc.RecordCapacity(ctx, nodes, slot, 5*time.Minute); err != nil {
			t.Fatalf("seed capacity: %v", err)
		}
	}

	approx := func(name string, got, want float64) {
		t.Helper()
		if math.Abs(got-want) > 1e-6 {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}

	// --- statement (all cost bases) -----------------------------------------
	biller := bill.NewBiller(pool)
	stmt, err := biller.Render(ctx, "chen-lab", "", from, to, cfgFrom)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	// wl-a: 8 GPU x 2h = 16 GPU-hr @1.05 = 16.80 (onprem)
	// wl-b: 1 GPU x 1h onprem @1.05 = 1.05 ; 1 GPU x 1h aws @2.40 = 2.40
	// wl-c: orphaned, not billed.
	approx("statement total", stmt.Total, 20.25)
	approx("statement gpu-seconds", stmt.TotalGPUSeconds, 64800) // (16+1+1)h * 3600
	if len(stmt.Lines) != 3 {
		t.Errorf("statement lines = %d, want 3 (wl-a onprem, wl-b onprem, wl-b aws)", len(stmt.Lines))
	}
	sections := map[string]bill.Section{}
	for _, s := range stmt.Sections {
		sections[s.Class] = s
	}
	if len(sections) != 2 {
		t.Errorf("sections = %d, want 2 (onprem, aws-burst)", len(sections))
	}
	approx("onprem subtotal", sections["onprem"].Subtotal, 17.85) // 16.80 + 1.05
	approx("aws-burst subtotal", sections["aws-burst"].Subtotal, 2.40)
	if stmt.Provisional {
		t.Error("period ended in the past; statement must not be provisional")
	}

	// --- scoped statement: AWS burst only -----------------------------------
	awsOnly, err := biller.Render(ctx, "chen-lab", "aws-burst", from, to, cfgFrom)
	if err != nil {
		t.Fatalf("render aws-burst: %v", err)
	}
	approx("aws-burst-only total", awsOnly.Total, 2.40)
	if len(awsOnly.Lines) != 1 {
		t.Errorf("aws-burst lines = %d, want 1", len(awsOnly.Lines))
	}

	// --- reconciliation -----------------------------------------------------
	recon := bill.NewReconciler(pool)
	r, err := recon.Run(ctx, from, to)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	approx("recon billed", r.TotalBilled, 20.25)
	approx("recon allocated", r.TotalAllocatedHours, 18) // 16 + 1 + 1
	approx("recon capacity", r.TotalCapacityHours, 48)   // (16 GPU + 8 GPU) present for 2h
	approx("recon idle", r.TotalIdleHours, 30)           // 48 - 18; NOT summed with unbilled
	if len(r.Unbilled) != 0 {
		t.Errorf("unbilled = %d, want 0 (every used pool is mapped and rated)", len(r.Unbilled))
	}
	if len(r.Orphans) != 1 || r.Orphans[0].WorkloadID != "wl-c" {
		t.Errorf("orphans = %+v, want exactly wl-c", r.Orphans)
	}
	if r.Clean() {
		t.Error("period has an orphan pod; Clean() must be false")
	}

	// --- health instruments read the same gaps -----------------------------
	// The /healthz and /metrics readings must agree with the reconciliation:
	// same orphan backlog, and unbilled read through the SAME Biller.Unbilled
	// predicate. A poll just completed, so it is not stale.
	health := bill.NewHealth(pool)
	snap, err := health.Snapshot(ctx, base.Add(90*time.Minute), 24*time.Hour)
	if err != nil {
		t.Fatalf("health snapshot: %v", err)
	}
	if !snap.EverPolled {
		t.Error("EverPolled = false after a completed poll")
	}
	if snap.OrphanPods != 1 {
		t.Errorf("health OrphanPods = %d, want 1 (agrees with reconciliation)", snap.OrphanPods)
	}
	if snap.UnbilledGaps != 0 || snap.UnbilledGPUHours != 0 {
		t.Errorf("health unbilled = %d gaps / %v hrs, want 0/0", snap.UnbilledGaps, snap.UnbilledGPUHours)
	}

	// --- idempotency: a second poll changes NOTHING -------------------------
	rowsBefore := countUsageHours(ctx, t, pool)
	if err := poll(ctx, log, rc, st, nc); err != nil {
		t.Fatalf("second poll: %v", err)
	}
	if got := countUsageHours(ctx, t, pool); got != rowsBefore {
		t.Errorf("usage_hour rows after replay = %d, want %d (poll must be idempotent)", got, rowsBefore)
	}
	stmt2, err := biller.Render(ctx, "chen-lab", "", from, to, cfgFrom)
	if err != nil {
		t.Fatalf("render after replay: %v", err)
	}
	approx("statement total after replay", stmt2.Total, 20.25)
	r2, err := recon.Run(ctx, from, to)
	if err != nil {
		t.Fatalf("reconcile after replay: %v", err)
	}
	approx("capacity after replay", r2.TotalCapacityHours, 48) // COUNT of observations, not a sum

	// --- freeze: a closed period reads back identical, from statement_line ---
	sid, err := biller.Close(ctx, "chen-lab", "", from, to)
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	var lineCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM statement_line WHERE statement_id=$1`, sid).Scan(&lineCount); err != nil {
		t.Fatalf("count lines: %v", err)
	}
	if lineCount != 3 {
		t.Errorf("frozen lines = %d, want 3", lineCount)
	}
	frozen, err := biller.Render(ctx, "chen-lab", "", from, to, cfgFrom)
	if err != nil {
		t.Fatalf("render frozen: %v", err)
	}
	if frozen.Provisional {
		t.Error("closed period must not be provisional")
	}
	approx("frozen total", frozen.Total, 20.25)
}

func countUsageHours(ctx context.Context, t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM usage_hour`).Scan(&n); err != nil {
		t.Fatalf("count usage_hour: %v", err)
	}
	return n
}
