package bill

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Reconciliation answers the operator's question, which is not the PI's.
//
//	capacity - allocated = IDLE      money burning, nobody's fault, real
//	allocated - billed   = UNBILLED  configuration gap, someone's fault, fixable
//
// The two gaps are different in kind and must never be added together. Idle is
// an operational fact you manage with scheduling and scale-to-zero. Unbilled is
// a bug you fix with an INSERT.
type Reconciliation struct {
	PeriodStart time.Time
	PeriodEnd   time.Time

	Capacities []CapacityLine
	Unbilled   []Unbilled
	Orphans    []Orphan

	TotalCapacityHours  float64
	TotalAllocatedHours float64
	TotalIdleHours      float64
	TotalUnbilledHours  float64
	TotalBilled         float64
}

// CapacityLine is one cost basis: what we had, used, and billed.
type CapacityLine struct {
	Class     string
	ClassName string
	ClusterID string
	NodePool  string
	GPUModel  string

	CapacityHours  float64
	AllocatedHours float64
	BilledHours    float64
	Billed         float64
}

func (c CapacityLine) IdleHours() float64     { return c.CapacityHours - c.AllocatedHours }
func (c CapacityLine) UnbilledHours() float64 { return c.AllocatedHours - c.BilledHours }

func (c CapacityLine) Utilization() float64 {
	if c.CapacityHours <= 0 {
		return 0
	}
	return c.AllocatedHours / c.CapacityHours
}

// Orphan is a pod we could not place. It is usage that exists and is not on any
// statement, because we refused to guess where it ran.
type Orphan struct {
	WorkloadID   string
	WorkloadName string
	PodName      string
	NodeName     string
	Submitter    string
	Project      string
	StartedAt    time.Time
}

type Reconciler struct{ db *pgxpool.Pool }

func NewReconciler(db *pgxpool.Pool) *Reconciler { return &Reconciler{db: db} }

// capacityQuery joins what we HAD (node_hour) against what got USED
// (usage_hour) and what got BILLED (usage joined to a rate).
//
// FULL OUTER JOIN, deliberately. A pool with capacity and zero usage must still
// appear — that is 100% idle, and it is exactly the row the CIO needs to see. An
// inner join would hide it, which is how an idle cloud pool bills nobody and
// nobody notices.
const capacityQuery = `
WITH cap AS (
    SELECT cluster_id, node_pool, gpu_model,
           sum(gpu_hours) AS capacity_hours
    FROM node_capacity
    WHERE hour_start >= $1 AND hour_start < $2
    GROUP BY cluster_id, node_pool, gpu_model
),
used AS (
    SELECT u.cluster_id, u.node_pool, u.gpu_model,
           sum(u.gpu_seconds) / 3600.0 AS allocated_hours,
           sum(u.gpu_seconds) FILTER (WHERE r.class_id IS NOT NULL) / 3600.0
               AS billed_hours,
           COALESCE(sum(u.gpu_seconds * r.usd_per_gpu_hour / 3600.0), 0)
               AS billed
    FROM usage_classified u
    LEFT JOIN rate r ON r.class_id  = u.class_id
                    AND r.gpu_model = u.gpu_model
                    AND u.hour_start >= r.effective_from
                    AND (r.effective_to IS NULL OR u.hour_start < r.effective_to)
    WHERE u.hour_start >= $1 AND u.hour_start < $2
    GROUP BY u.cluster_id, u.node_pool, u.gpu_model
)
SELECT
    COALESCE(pc.class_id, ''),
    COALESCE(rc.name, pc.class_id, 'UNMAPPED'),
    COALESCE(cap.cluster_id, used.cluster_id),
    COALESCE(cap.node_pool,  used.node_pool),
    COALESCE(cap.gpu_model,  used.gpu_model),
    COALESCE(cap.capacity_hours,  0),
    COALESCE(used.allocated_hours, 0),
    COALESCE(used.billed_hours,    0),
    COALESCE(used.billed,          0)
FROM cap
FULL OUTER JOIN used
     ON  used.cluster_id = cap.cluster_id
     AND used.node_pool  = cap.node_pool
     AND used.gpu_model  = cap.gpu_model
-- Exact mapping wins over the wildcard. Joining on (exact OR '*') produced TWO
-- rows when both existed, silently DOUBLING capacity. usage_classified gets
-- this right with COALESCE; this query has to as well. Same precedence, once.
LEFT JOIN LATERAL (
    SELECT pc.class_id
    FROM pool_class pc
    WHERE pc.cluster_id = COALESCE(cap.cluster_id, used.cluster_id)
      AND pc.node_pool IN (COALESCE(cap.node_pool, used.node_pool), '*')
      AND $1 >= pc.effective_from
      AND (pc.effective_to IS NULL OR $1 < pc.effective_to)
    ORDER BY (pc.node_pool = '*')      -- false sorts first: exact beats wildcard
    LIMIT 1
) pc ON true
LEFT JOIN rate_class rc ON rc.class_id = pc.class_id
ORDER BY 1, 3, 4, 5
`

func (rec *Reconciler) Run(ctx context.Context, from, to time.Time) (*Reconciliation, error) {
	r := &Reconciliation{PeriodStart: from, PeriodEnd: to}

	rows, err := rec.db.Query(ctx, capacityQuery, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var c CapacityLine
		if err := rows.Scan(&c.Class, &c.ClassName, &c.ClusterID, &c.NodePool,
			&c.GPUModel, &c.CapacityHours, &c.AllocatedHours,
			&c.BilledHours, &c.Billed); err != nil {
			return nil, err
		}
		r.Capacities = append(r.Capacities, c)

		r.TotalCapacityHours += c.CapacityHours
		r.TotalAllocatedHours += c.AllocatedHours
		r.TotalUnbilledHours += c.UnbilledHours()
		r.TotalBilled += c.Billed
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	r.TotalIdleHours = r.TotalCapacityHours - r.TotalAllocatedHours

	b := &Biller{db: rec.db}
	if r.Unbilled, err = b.Unbilled(ctx, from, to); err != nil {
		return nil, err
	}
	if r.Orphans, err = rec.orphans(ctx, from, to); err != nil {
		return nil, err
	}

	return r, nil
}

func (rec *Reconciler) orphans(ctx context.Context, from, to time.Time) ([]Orphan, error) {
	rows, err := rec.db.Query(ctx, `
		SELECT workload_id, COALESCE(workload_name,''), pod_name,
		       COALESCE(node_name,''), COALESCE(submitter,''),
		       COALESCE(project,''), COALESCE(started_at, first_seen)
		FROM orphan_pod
		WHERE NOT resolved
		  AND COALESCE(started_at, first_seen) >= $1
		  AND COALESCE(started_at, first_seen) <  $2
		ORDER BY started_at
	`, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Orphan
	for rows.Next() {
		var o Orphan
		if err := rows.Scan(&o.WorkloadID, &o.WorkloadName, &o.PodName,
			&o.NodeName, &o.Submitter, &o.Project, &o.StartedAt); err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// Clean reports whether the period can be closed without leaving money on the
// floor. Idle does NOT make a period dirty — idle is an operational fact, not a
// bug. Unbilled hours and orphaned pods do.
func (r *Reconciliation) Clean() bool {
	return len(r.Unbilled) == 0 && len(r.Orphans) == 0
}
