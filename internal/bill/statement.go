// Package bill turns the ledger into statements.
//
// The statement is a QUERY RESULT, not a document. The same query backs the web
// page, the PDF, the CSV the research office wants, and the API. There is one
// write in the whole system — Close() — and after it runs the period is
// immutable and reproducible under audit.
package bill

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Line struct {
	WorkloadID  string // carried into statement_line: a frozen line must be traceable
	Date        time.Time
	Submitter   string
	Workload    string
	Type        string
	Description string // from annotations; empty means policy wasn't enforced
	FundCode    string
	GPUModel    string
	NodePool    string // where it actually ran, not where it asked to run
	Class       string // the cost basis it ran against
	ClassName   string
	GPUAlloc    float64
	GPUSeconds  float64
	UtilMean    *float64 // nil when the metric series was unavailable
	Rate        float64
	Amount      float64
	Running     float64 // cumulative within this statement
}

type Statement struct {
	GroupID     string
	GroupName   string
	PeriodStart time.Time
	PeriodEnd   time.Time

	// Class scopes the statement to one rate class -- one set of node pools
	// sharing a cost basis. Empty means every class the group touched.
	Class     string
	ClassName string

	// Provisional means the period is still open: this is a live view of the
	// ledger, not a frozen artifact. Say so on the page. PIs will check it
	// weekly, which is exactly what you want.
	Provisional bool

	Lines []Line

	// Sections are the same lines, grouped by cost basis. A group that ran
	// on-prem AND in AWS this month needs to see BOTH costs, separated, with
	// their own subtotals -- not a single blended number that hides which
	// capacity the money went to.
	Sections []Section

	TotalGPUSeconds float64
	Total           float64
	FiscalYTD       float64 // different query. Carry both.
}

// Section is one cost basis within a statement.
type Section struct {
	Class      string
	ClassName  string
	Lines      []Line
	GPUSeconds float64
	Subtotal   float64

	// Rates seen in this section. Usually one. More than one means a rate
	// change landed mid-period, and the line-level rate is the blended figure.
	Rates []string
}

// The join that is the whole system.
//
// Note the rate join is on hour_start, not on the workload's start time. That
// is the entire reason the ledger grain is hourly: a rate change mid-period is
// applied to the hours after it, with no splitting logic anywhere.
const linesQuery = `
WITH scoped AS (
    SELECT
        u.workload_id,
        u.gpu_model,
        u.hour_start,
        u.gpu_alloc,
        u.gpu_seconds,
        u.gpu_util_mean,
        u.node_pool,
        u.class_id,
        r.usd_per_gpu_hour AS rate
    FROM usage_classified u
    JOIN workload w      ON w.workload_id = u.workload_id
    JOIN project_group g ON g.project = w.project
                        AND u.hour_start >= g.effective_from
                        AND (g.effective_to IS NULL OR u.hour_start < g.effective_to)
    JOIN rate r          ON r.class_id  = u.class_id
                        AND r.gpu_model = u.gpu_model
                        AND u.hour_start >= r.effective_from
                        AND (r.effective_to IS NULL OR u.hour_start < r.effective_to)
    WHERE g.group_id  = $1
      AND u.hour_start >= $2
      AND u.hour_start <  $3
      -- $4 is the scope. NULL = every rate class the group touched (one
      -- statement covering all capacity). A class_id = one statement for that
      -- capacity alone -- e.g. bill the AWS pool separately from on-prem.
      AND ($4::text IS NULL OR u.class_id = $4)
)
SELECT
    s.workload_id,
    w.started_at,
    w.submitter,
    w.name,
    COALESCE(w.workload_type, ''),
    COALESCE(w.annotations->>'recharge.description', ''),
    COALESCE(w.annotations->>'recharge.fund-code', ''),
    s.gpu_model,
    s.node_pool,
    s.class_id,
    COALESCE(rc.name, s.class_id),
    max(s.gpu_alloc),
    sum(s.gpu_seconds),

    -- Utilization weighted by the GPU-seconds it applies to. An unweighted mean
    -- lets a two-minute idle hour drag down a two-week job.
    CASE WHEN sum(s.gpu_seconds) FILTER (WHERE s.gpu_util_mean IS NOT NULL) > 0
         THEN sum(s.gpu_util_mean * s.gpu_seconds)
              FILTER (WHERE s.gpu_util_mean IS NOT NULL)
              / sum(s.gpu_seconds) FILTER (WHERE s.gpu_util_mean IS NOT NULL)
    END,

    -- Effective rate, GPU-second-weighted, so a mid-period rate change shows a
    -- blended number on the line rather than lying about either rate.
    sum(s.rate * s.gpu_seconds) / NULLIF(sum(s.gpu_seconds), 0),

    sum(s.gpu_seconds * s.rate / 3600.0) AS amount
FROM scoped s
JOIN workload w        ON w.workload_id = s.workload_id
LEFT JOIN rate_class rc ON rc.class_id = s.class_id
GROUP BY w.started_at, w.submitter, w.name, w.workload_type,
         w.annotations, s.gpu_model, s.node_pool, s.class_id, rc.name,
         s.workload_id
-- Order by class first. A workload preempted out of on-prem and rescheduled
-- into AWS produces a line under EACH -- which is correct, and the statement
-- should show it rather than hide it behind a single blended figure.
ORDER BY s.class_id, w.started_at, w.submitter, w.name
`

type Biller struct{ db *pgxpool.Pool }

func NewBiller(db *pgxpool.Pool) *Biller { return &Biller{db: db} }

// Render returns a statement.
//
// If the period has been CLOSED, this reads the frozen statement_line rows. If
// it has not, it derives from the live ledger and marks the result provisional.
//
// That distinction is the entire reason Close() exists. Do not "simplify" this
// to always derive from the ledger: a late poll or a rate correction would then
// silently rewrite a closed period, and the frozen rate_applied would be dead
// weight. A frozen rate that nobody reads back is not a frozen rate.
func (b *Biller) Render(ctx context.Context, groupID, class string, from, to time.Time, fyStart time.Time) (*Statement, error) {
	sid, err := b.closedStatement(ctx, groupID, class, from, to)
	if err != nil {
		return nil, err
	}
	if sid != 0 {
		return b.renderFrozen(ctx, sid, groupID, class, from, to, fyStart)
	}
	return b.renderLive(ctx, groupID, class, from, to, fyStart)
}

func (b *Biller) closedStatement(ctx context.Context, groupID, class string, from, to time.Time) (int64, error) {
	var cls *string
	if class != "" {
		cls = &class
	}
	var sid int64
	err := b.db.QueryRow(ctx, `
		SELECT statement_id FROM statement
		WHERE group_id = $1 AND period_start = $2 AND period_end = $3
		  AND class_id IS NOT DISTINCT FROM $4
	`, groupID, from, to, cls).Scan(&sid)
	if err != nil {
		return 0, nil // not closed
	}
	return sid, nil
}

// renderFrozen reads a closed period back out of statement_line.
//
// Nothing here touches usage_hour or rate. The numbers are what they were the
// day the period was closed, and they will be the same in FY29.
func (b *Biller) renderFrozen(ctx context.Context, sid int64, groupID, class string, from, to, fyStart time.Time) (*Statement, error) {
	st := &Statement{
		GroupID:     groupID,
		Class:       class,
		PeriodStart: from,
		PeriodEnd:   to,
		Provisional: false,
	}

	if err := b.db.QueryRow(ctx,
		`SELECT name FROM billing_group WHERE group_id = $1`, groupID).
		Scan(&st.GroupName); err != nil {
		return nil, err
	}

	rows, err := b.db.Query(ctx, `
		SELECT sl.workload_id, sl.started_at, sl.submitter, sl.workload_name,
		       COALESCE(sl.workload_type,''), COALESCE(sl.description,''),
		       COALESCE(sl.fund_code,''), sl.gpu_model,
		       COALESCE(sl.node_pool,''), COALESCE(sl.class_id,''),
		       COALESCE(rc.name, sl.class_id, ''),
		       sl.gpu_alloc, sl.gpu_seconds, sl.gpu_util_mean,
		       sl.rate_applied, sl.amount_usd
		FROM statement_line sl
		LEFT JOIN rate_class rc ON rc.class_id = sl.class_id
		WHERE sl.statement_id = $1
		ORDER BY sl.line_no
	`, sid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var lines []Line
	for rows.Next() {
		var l Line
		if err := rows.Scan(&l.WorkloadID, &l.Date, &l.Submitter, &l.Workload,
			&l.Type, &l.Description, &l.FundCode, &l.GPUModel, &l.NodePool,
			&l.Class, &l.ClassName, &l.GPUAlloc, &l.GPUSeconds, &l.UtilMean,
			&l.Rate, &l.Amount); err != nil {
			return nil, err
		}
		lines = append(lines, l)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	collate(st, lines)

	// Fiscal YTD spans closed AND open periods, so it is necessarily derived.
	// It is a running figure, not a frozen one.
	ytd, err := b.Render2(ctx, groupID, class, fyStart, to)
	if err != nil {
		return nil, err
	}
	st.FiscalYTD = ytd

	return st, nil
}

// renderLive derives a statement from the ledger. Used for open periods, and by
// Close() to compute what it is about to freeze.
func (b *Biller) renderLive(ctx context.Context, groupID, class string, from, to time.Time, fyStart time.Time) (*Statement, error) {
	st := &Statement{
		GroupID:     groupID,
		Class:       class,
		PeriodStart: from,
		PeriodEnd:   to,
		Provisional: time.Now().Before(to),
	}

	var scope *string
	if class != "" {
		scope = &class
		_ = b.db.QueryRow(ctx,
			`SELECT name FROM rate_class WHERE class_id = $1`, class).Scan(&st.ClassName)
	}

	err := b.db.QueryRow(ctx,
		`SELECT name FROM billing_group WHERE group_id = $1`, groupID).
		Scan(&st.GroupName)
	if err != nil {
		return nil, err
	}

	rows, err := b.db.Query(ctx, linesQuery, groupID, from, to, scope)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var lines []Line
	for rows.Next() {
		var l Line
		if err := rows.Scan(&l.WorkloadID, &l.Date, &l.Submitter, &l.Workload, &l.Type,
			&l.Description, &l.FundCode, &l.GPUModel, &l.NodePool,
			&l.Class, &l.ClassName, &l.GPUAlloc,
			&l.GPUSeconds, &l.UtilMean, &l.Rate, &l.Amount); err != nil {
			return nil, err
		}
		lines = append(lines, l)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	collate(st, lines)

	// Fiscal YTD is a separate query, not a running sum of the page. The PI
	// cares about this number more than the one at the bottom of the table.
	ytd, err := b.Render2(ctx, groupID, class, fyStart, to)
	if err != nil {
		return nil, err
	}
	st.FiscalYTD = ytd

	return st, nil
}

// Render2 is the scalar-total form, used for fiscal-YTD without materializing
// every line.
func (b *Biller) Render2(ctx context.Context, groupID, class string, from, to time.Time) (float64, error) {
	var scope *string
	if class != "" {
		scope = &class
	}
	var total *float64
	err := b.db.QueryRow(ctx, `
		SELECT sum(u.gpu_seconds * r.usd_per_gpu_hour / 3600.0)
		FROM usage_classified u
		JOIN workload w      ON w.workload_id = u.workload_id
		JOIN project_group g ON g.project = w.project
		                    AND u.hour_start >= g.effective_from
		                    AND (g.effective_to IS NULL OR u.hour_start < g.effective_to)
		JOIN rate r          ON r.class_id  = u.class_id
		                    AND r.gpu_model = u.gpu_model
		                    AND u.hour_start >= r.effective_from
		                    AND (r.effective_to IS NULL OR u.hour_start < r.effective_to)
		WHERE g.group_id = $1 AND u.hour_start >= $2 AND u.hour_start < $3
		  AND ($4::text IS NULL OR u.class_id = $4)
	`, groupID, from, to, scope).Scan(&total)
	if err != nil {
		return 0, err
	}
	if total == nil {
		return 0, nil
	}
	return *total, nil
}

// Close freezes a period. rate_applied is written as a VALUE, not a foreign key
// to a rate row that may since have changed. Somebody will audit FY26 in FY29.
//
// This is the only write in the billing path. It is idempotent by virtue of the
// UNIQUE constraint on (period_start, period_end, group_id) — re-running Close
// on a closed period is an error, not a silent overwrite.
func (b *Biller) Close(ctx context.Context, groupID, class string, from, to time.Time) (int64, error) {
	// renderLive, not Render: Render would find the statement we are about to
	// write and read it back. Close freezes the LEDGER.
	st, err := b.renderLive(ctx, groupID, class, from, to, from)
	if err != nil {
		return 0, err
	}

	tx, err := b.db.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	var sid int64
	var cls *string
	if class != "" {
		cls = &class
	}
	err = tx.QueryRow(ctx, `
		INSERT INTO statement (period_start, period_end, group_id, class_id)
		VALUES ($1,$2,$3,$4) RETURNING statement_id
	`, from, to, groupID, cls).Scan(&sid)
	if err != nil {
		return 0, err
	}

	for i, l := range st.Lines {
		_, err = tx.Exec(ctx, `
			INSERT INTO statement_line (
				statement_id, line_no, workload_id, submitter, workload_name,
				workload_type, description, fund_code, gpu_model, node_pool,
				class_id, gpu_alloc, started_at, gpu_seconds, gpu_util_mean,
				rate_applied, amount_usd)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
		`, sid, i+1, l.WorkloadID, l.Submitter, l.Workload, l.Type, l.Description,
			l.FundCode, l.GPUModel, l.NodePool, cls, l.GPUAlloc, l.Date,
			l.GPUSeconds, l.UtilMean, l.Rate, l.Amount)
		if err != nil {
			return 0, err
		}
	}

	return sid, tx.Commit(ctx)
}

// ---------------------------------------------------------------------------
// usage — what got used, and what it billed. That is all.
// ---------------------------------------------------------------------------

// Usage is the operational summary: GPU-hours and dollars, by cost basis and
// GPU model. Useful at rate-review time, but the tool does not tell you what
// the rate should be. You set the rate.
type Usage struct {
	Class     string
	ClassName string
	GPUModel  string
	GPUHours  float64
	Billed    float64
}

// Unbilled is GPU-hours in the ledger that failed to join a rate.
//
// The join drops them silently, which means they are usage nobody is paying
// for and nobody is looking at. There are exactly two causes:
//
//   - the node pool has no pool_class mapping  (class_id IS NULL)
//   - the (class, gpu_model) pair has no rate for that hour
//
// Both are configuration gaps, not data problems. Both are revenue on the floor.
// This is the reconciliation that catches them.
type Unbilled struct {
	ClusterID string
	NodePool  string
	GPUModel  string
	Class     string // empty = no pool_class mapping at all
	GPUHours  float64
	Reason    string
}

func (b *Biller) Unbilled(ctx context.Context, from, to time.Time) ([]Unbilled, error) {
	rows, err := b.db.Query(ctx, `
		SELECT
			u.cluster_id,
			u.node_pool,
			u.gpu_model,
			COALESCE(u.class_id, ''),
			sum(u.gpu_seconds) / 3600.0,
			CASE WHEN u.class_id IS NULL
			     THEN 'no pool_class mapping for this cluster/pool'
			     ELSE 'no rate for (' || u.class_id || ', ' || u.gpu_model || ') in this period'
			END
		FROM usage_classified u
		LEFT JOIN rate r ON r.class_id  = u.class_id
		                AND r.gpu_model = u.gpu_model
		                AND u.hour_start >= r.effective_from
		                AND (r.effective_to IS NULL OR u.hour_start < r.effective_to)
		WHERE u.hour_start >= $1 AND u.hour_start < $2
		  AND r.class_id IS NULL          -- the join failed
		GROUP BY u.cluster_id, u.node_pool, u.gpu_model, u.class_id
		ORDER BY 5 DESC
	`, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Unbilled
	for rows.Next() {
		var u Unbilled
		if err := rows.Scan(&u.ClusterID, &u.NodePool, &u.GPUModel,
			&u.Class, &u.GPUHours, &u.Reason); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

func (b *Biller) Usage(ctx context.Context, from, to time.Time) ([]Usage, error) {
	rows, err := b.db.Query(ctx, `
		SELECT
			u.class_id,
			COALESCE(rc.name, u.class_id),
			u.gpu_model,
			sum(u.gpu_seconds) / 3600.0,
			sum(u.gpu_seconds * r.usd_per_gpu_hour / 3600.0)
		FROM usage_classified u
		JOIN rate r             ON r.class_id  = u.class_id
		                       AND r.gpu_model = u.gpu_model
		                       AND u.hour_start >= r.effective_from
		                       AND (r.effective_to IS NULL OR u.hour_start < r.effective_to)
		LEFT JOIN rate_class rc ON rc.class_id = u.class_id
		WHERE u.hour_start >= $1 AND u.hour_start < $2
		GROUP BY u.class_id, rc.name, u.gpu_model
		ORDER BY u.class_id, u.gpu_model
	`, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Usage
	for rows.Next() {
		var u Usage
		if err := rows.Scan(&u.Class, &u.ClassName, &u.GPUModel,
			&u.GPUHours, &u.Billed); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

// collate groups lines into sections by cost basis and computes running totals.
//
// Both read paths -- live and frozen -- produce the same shape, so they share
// this. It is the ONLY thing they share: the two paths must stay behaviorally
// distinct, because one is derived and one is immutable.
func collate(st *Statement, lines []Line) {
	var running float64
	var sections []*Section
	byClass := map[string]*Section{}

	for _, l := range lines {
		running += l.Amount
		l.Running = running

		st.TotalGPUSeconds += l.GPUSeconds
		st.Lines = append(st.Lines, l)

		sec, ok := byClass[l.Class]
		if !ok {
			sec = &Section{Class: l.Class, ClassName: l.ClassName}
			byClass[l.Class] = sec
			sections = append(sections, sec)
		}
		sec.Lines = append(sec.Lines, l)
		sec.GPUSeconds += l.GPUSeconds
		sec.Subtotal += l.Amount

		note := fmt.Sprintf("%s $%.2f/GPU-hr", l.GPUModel, l.Rate)
		if !slices.Contains(sec.Rates, note) {
			sec.Rates = append(sec.Rates, note)
		}
	}

	st.Total = running
	for _, sec := range sections {
		st.Sections = append(st.Sections, *sec)
	}
}
