package ledger

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/scttfrdmn/runai-recharge/internal/runai"
)

// Placement is one (where, what-GPU) group of a workload's pods, with the
// interval and allocation those pods actually occupied.
//
// A workload usually has exactly one placement. It has more than one when a job
// was preempted out of a pool and rescheduled into another — which the hourly
// grain handles by producing rows in both, summing correctly, with no special
// case anywhere.
type Placement struct {
	ClusterID string
	NodePool  string
	GPUModel  string

	Start time.Time
	End   time.Time // zero if still running
	Alloc float64   // SUM of GPUs across the pods in this group
}

// NodeCache resolves node_name -> (cluster, pool, gpu_model).
//
// Nodes are never deleted. In AWS they scale to zero and return with new names;
// a node that vanished between polls must still resolve for the hours it
// served, because a statement re-run in FY29 has to reproduce FY26 exactly.
type NodeCache struct{ db *pgxpool.Pool }

func NewNodeCache(db *pgxpool.Pool) *NodeCache { return &NodeCache{db: db} }

// Refresh upserts the current node inventory. Call before ingesting workloads.
func (nc *NodeCache) Refresh(ctx context.Context, nodes []runai.Node) error {
	tx, err := nc.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	for _, n := range nodes {
		// A node with no resolvable GPU model would land an empty string in
		// usage_hour, the rate join would find no match, and the row would
		// SILENTLY VANISH from the statement -- not billed, not flagged.
		//
		// There is no safe default here. A default is a guess, and a guess in
		// this position is unbilled revenue that nobody notices.
		if n.GPUModel() == "" {
			if n.GPUCount() == 0 {
				continue // CPU-only node. Correctly not billable.
			}
			return fmt.Errorf(
				"node %q reports %d GPUs but no GPU model; "+
					"check nvidia.com/gpu.product on the node. Refusing to guess",
				n.Name, n.GPUCount())
		}

		_, err = tx.Exec(ctx, `
			INSERT INTO node (node_name, cluster_id, node_pool, gpu_model, gpu_count, seen_at)
			VALUES ($1,$2,$3,$4,$5, now())
			ON CONFLICT (node_name) DO UPDATE SET
				cluster_id = EXCLUDED.cluster_id,
				node_pool  = EXCLUDED.node_pool,
				gpu_model  = EXCLUDED.gpu_model,
				gpu_count  = EXCLUDED.gpu_count,
				seen_at    = now()
		`, n.Name, n.ClusterID, n.NodePool, n.GPUModel(), n.GPUCount())
		if err != nil {
			return err
		}

		// No history table. usage_hour freezes (cluster, pool, gpu_model) at
		// ingest, so a node later recycled into a different pool cannot
		// retroactively rewrite the hours it already served. A separate history
		// table was written and never read.
	}
	return tx.Commit(ctx)
}

// RecordCapacity records one observation per present GPU node, keyed by poll
// slot.
//
// Capacity is a COUNT of distinct (node, slot) observations, never a running
// sum. DO NOT accumulate a presence fraction here: a sum inflates on replay, and
// replay-safety is the invariant every other write in the poll path depends on.
// One non-idempotent write is worse than none, because it makes the guarantee
// untrue while looking like it holds.
func (nc *NodeCache) RecordCapacity(ctx context.Context, nodes []runai.Node, now time.Time, pollInterval time.Duration) error {
	slot := now.UTC().Truncate(pollInterval)
	secs := int(pollInterval.Seconds())

	tx, err := nc.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	for _, n := range nodes {
		if n.GPUCount() == 0 {
			continue // CPU-only. Not capacity we recharge for.
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO node_observation (node_name, slot_start, slot_seconds,
			                              cluster_id, node_pool, gpu_model, gpu_count)
			VALUES ($1,$2,$3,$4,$5,$6,$7)
			ON CONFLICT (node_name, slot_start) DO UPDATE SET
				gpu_count = EXCLUDED.gpu_count
		`, n.Name, slot, secs, n.ClusterID, n.NodePool, n.GPUModel(), n.GPUCount())
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// RecordOrphans persists pods we could not place.
//
// This was a log line. A billing gap that exists only in stderr is a billing gap
// nobody will ever find. It goes on the reconciliation statement, where an
// operator has to look at it.
func (s *Store) RecordOrphans(ctx context.Context, w WorkloadRecord, pods []runai.Pod) error {
	for _, p := range pods {
		_, err := s.db.Exec(ctx, `
			INSERT INTO orphan_pod (workload_id, pod_name, node_name, workload_name,
			                        submitter, project, started_at, first_seen, last_seen)
			VALUES ($1,$2,$3,$4,$5,$6,$7, now(), now())
			ON CONFLICT (workload_id, pod_name) DO UPDATE SET last_seen = now()
		`, w.ID, p.Name, p.NodeName, w.Name, w.Submitter, w.Project, p.StartedAt)
		if err != nil {
			return err
		}
	}
	return nil
}

// ResolveOrphans clears orphans for a workload once its pods place successfully
// -- which happens when a node reappears in the cache on a later poll.
func (s *Store) ResolveOrphans(ctx context.Context, workloadID string) error {
	_, err := s.db.Exec(ctx,
		`UPDATE orphan_pod SET resolved = true WHERE workload_id = $1`, workloadID)
	return err
}

type nodeInfo struct{ cluster, pool, model string }

// Resolve groups a workload's pods by where they actually ran.
//
// If a pod is on a node we've never seen — the node was scaled away before we
// polled — it is returned in `unresolved` rather than guessed at. Guessing
// placement is the one thing this system must never do: it is the difference
// between an auditable statement and a plausible one.
func (nc *NodeCache) Resolve(ctx context.Context, pods []runai.Pod, fallbackStart time.Time, fallbackAlloc float64) (placements []Placement, unresolved []runai.Pod, err error) {
	if len(pods) == 0 {
		return nil, nil, nil
	}

	names := make([]string, 0, len(pods))
	for _, p := range pods {
		if p.NodeName != "" {
			names = append(names, p.NodeName)
		}
	}

	rows, err := nc.db.Query(ctx,
		`SELECT node_name, cluster_id, node_pool, gpu_model
		   FROM node WHERE node_name = ANY($1)`, names)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	lookup := map[string]nodeInfo{}
	for rows.Next() {
		var n string
		var i nodeInfo
		if err := rows.Scan(&n, &i.cluster, &i.pool, &i.model); err != nil {
			return nil, nil, err
		}
		lookup[n] = i
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	// Group pods by (cluster, pool, model). Sum their allocation; take the
	// earliest start and the latest end.
	type key struct{ c, p, m string }
	agg := map[key]*Placement{}

	// A placement is still running if ANY of its pods is. Deciding this inside
	// the loop made the answer depend on iteration order: a completed pod seen
	// after a running one would win, and a live job would be billed as
	// finished. Track it explicitly.
	running := map[key]bool{}

	for _, pod := range pods {
		ni, ok := lookup[pod.NodeName]
		if !ok {
			unresolved = append(unresolved, pod)
			continue
		}

		k := key{ni.cluster, ni.pool, ni.model}
		pl, ok := agg[k]
		if !ok {
			pl = &Placement{ClusterID: ni.cluster, NodePool: ni.pool, GPUModel: ni.model}
			agg[k] = pl
		}

		// Per-pod GPU request is the number to trust. The workload-level
		// GPU_ALLOCATION metric may or may not be the sum across workers --
		// see verify-alloc. Summing pods sidesteps the ambiguity entirely.
		if pod.RequestedGPUs != nil {
			pl.Alloc += *pod.RequestedGPUs
		} else {
			pl.Alloc += fallbackAlloc
		}

		start := fallbackStart
		if pod.StartedAt != nil {
			start = *pod.StartedAt
		}
		if pl.Start.IsZero() || start.Before(pl.Start) {
			pl.Start = start
		}

		if pod.CompletedAt == nil {
			running[k] = true
		} else if pod.CompletedAt.After(pl.End) {
			pl.End = *pod.CompletedAt
		}
	}

	for k, pl := range agg {
		if running[k] {
			pl.End = time.Time{} // caller slices to now
		}
		placements = append(placements, *pl)
	}
	return placements, unresolved, nil
}
