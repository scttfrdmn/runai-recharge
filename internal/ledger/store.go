package ledger

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct{ db *pgxpool.Pool }

func NewStore(db *pgxpool.Pool) *Store { return &Store{db: db} }

// WorkloadRecord is the identity half of an ingest.
type WorkloadRecord struct {
	ID          string
	Submitter   string
	Project     string
	Department  string
	ClusterID   string
	NodePool    string
	Name        string
	Type        string
	Image       string
	Annotations map[string]string
	StartedAt   time.Time
	CompletedAt *time.Time
	Phase       string
}

// Ingest writes one workload and its hourly buckets in a single transaction.
//
// Idempotent on (workload_id, gpu_model, hour_start). The poller can crash,
// double-run, or replay a window without corrupting the ledger. A workload
// still in flight has its open hour REWRITTEN on each poll, not appended to —
// which is why running jobs need no special handling.
func (s *Store) Ingest(ctx context.Context, w WorkloadRecord, ps []Placement, util map[time.Time]float64, now time.Time) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		INSERT INTO workload (
			workload_id, submitter, project, department, cluster_id, node_pool,
			name, workload_type, image, annotations,
			started_at, completed_at, phase, last_polled_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13, now())
		ON CONFLICT (workload_id) DO UPDATE SET
			completed_at   = EXCLUDED.completed_at,
			phase          = EXCLUDED.phase,
			annotations    = EXCLUDED.annotations,
			last_polled_at = now()
	`, w.ID, w.Submitter, w.Project, nullStr(w.Department), w.ClusterID,
		nullStr(w.NodePool), w.Name, nullStr(w.Type), nullStr(w.Image),
		w.Annotations, w.StartedAt, w.CompletedAt, w.Phase)
	if err != nil {
		return err
	}

	// One placement per (cluster, pool, gpu_model) the workload's pods actually
	// occupied. Usually exactly one. More than one when a job was preempted out
	// of a pool and rescheduled into another -- which needs no special case,
	// because the grain already carries placement.
	for _, pl := range ps {
		end := pl.End
		if end.IsZero() {
			end = now // still running
		}

		for _, b := range Slice(pl.Start, end, pl.Alloc) {
			var u *float64
			if v, ok := util[b.HourStart]; ok {
				u = &v
			}

			_, err = tx.Exec(ctx, `
				INSERT INTO usage_hour (
					workload_id, cluster_id, node_pool, gpu_model, hour_start,
					gpu_alloc, seconds, gpu_seconds, gpu_util_mean, updated_at)
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9, now())
				ON CONFLICT (workload_id, cluster_id, node_pool, gpu_model, hour_start)
				DO UPDATE SET
					gpu_alloc     = EXCLUDED.gpu_alloc,
					seconds       = EXCLUDED.seconds,
					gpu_seconds   = EXCLUDED.gpu_seconds,
					gpu_util_mean = COALESCE(EXCLUDED.gpu_util_mean, usage_hour.gpu_util_mean),
					updated_at    = now()
			`, w.ID, pl.ClusterID, pl.NodePool, pl.GPUModel, b.HourStart,
				pl.Alloc, b.Seconds, b.GPUSeconds, u)
			if err != nil {
				return err
			}
		}
	}

	return tx.Commit(ctx)
}

// Watermark returns the last point at which a poll completed IN FULL.
//
// DO NOT derive this from max(last_polled_at) over the workload table. It looks
// equivalent and it is not: a poll that ingests 1 of 500 workloads and then
// crashes would advance the watermark past the 499 it never wrote, and those
// fall outside every subsequent window. Permanently. The failure is silent.
//
// One row, advanced only by CommitWatermark, after the whole poll succeeds. If
// a poll dies mid-window the next run re-reads the entire window -- which is
// free, because every write in the poll path is idempotent.
func (s *Store) Watermark(ctx context.Context) (time.Time, error) {
	var t time.Time
	err := s.db.QueryRow(ctx, `SELECT watermark FROM poll_state WHERE id = 1`).Scan(&t)
	if err == pgx.ErrNoRows {
		return time.Time{}, nil // cold start
	}
	if err != nil {
		return time.Time{}, err
	}
	return t, nil
}

// CommitWatermark advances the watermark. Call this ONLY after a poll has
// ingested every workload in the window without error.
func (s *Store) CommitWatermark(ctx context.Context, t time.Time) error {
	_, err := s.db.Exec(ctx, `
		INSERT INTO poll_state (id, watermark, last_success)
		VALUES (1, $1, now())
		ON CONFLICT (id) DO UPDATE SET
			watermark    = EXCLUDED.watermark,
			last_success = now()
	`, t)
	return err
}

func nullStr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
