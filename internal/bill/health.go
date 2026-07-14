package bill

// Health reads the instruments the rest of the system writes. Every other guard
// in this codebase -- orphan_pod, Unbilled, the pagination tripwire, the poll
// watermark -- detects a gap and then terminates into a log line or a non-zero
// exit that a cron emails to nobody. This is where those readings become
// something a monitoring system can scrape and page on. An instrument nobody
// reads is not an instrument.
//
// The three signals, in order of how much it costs to not have them:
//
//   - poll staleness: last_success older than a few intervals. The poll is the
//     only irreplaceable component -- source data ages out of Run:ai and a gap
//     is unrecoverable. Note this ALSO covers the pagination tripwire and every
//     other poll failure for free: a poll that errors never reaches
//     CommitWatermark, so last_success simply stops advancing. Any way the poll
//     can fail shows up here as staleness.
//   - orphan pods: pods we refused to place, standing unresolved. Not urgent,
//     but must reach a human before close-of-month.
//   - unbilled GPU-hours: usage that joined no rate. A config gap that is
//     revenue on the floor until someone fixes a mapping.

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Health struct{ db *pgxpool.Pool }

func NewHealth(db *pgxpool.Pool) *Health { return &Health{db: db} }

// HealthSnapshot is a point-in-time reading of the instruments. Zero values are
// deliberate: a never-polled system reports EverPolled=false and a zero
// LastPollSuccess, which the staleness check treats as stale -- because nothing
// has yet proven the pipe works.
type HealthSnapshot struct {
	EverPolled      bool
	LastPollSuccess time.Time
	Watermark       time.Time

	OrphanPods int // unresolved orphan_pod rows, standing (not period-scoped)

	// Unbilled over a trailing window. Unbilled is inherently period-scoped
	// (the rate join is temporal), so it needs a window; UnbilledWindow records
	// which one this reading used.
	UnbilledGPUHours float64
	UnbilledGaps     int
	UnbilledWindow   time.Duration
}

// Snapshot reads poll_state, the standing orphan backlog, and unbilled hours
// over [now-unbilledWindow, now).
//
// The unbilled read goes through Biller.Unbilled -- the SAME predicate the
// reconciliation uses -- on purpose. A second, subtly-different definition of
// "unbilled" living in the health path is exactly the kind of drift that makes
// a dashboard disagree with the statement and nobody able to say which is right.
func (h *Health) Snapshot(ctx context.Context, now time.Time, unbilledWindow time.Duration) (HealthSnapshot, error) {
	s := HealthSnapshot{UnbilledWindow: unbilledWindow}

	var last, wm *time.Time
	err := h.db.QueryRow(ctx,
		`SELECT last_success, watermark FROM poll_state WHERE id = 1`).Scan(&last, &wm)
	if err != nil && err != pgx.ErrNoRows {
		return s, err
	}
	if err == nil {
		s.EverPolled = true
		if last != nil {
			s.LastPollSuccess = *last
		}
		if wm != nil {
			s.Watermark = *wm
		}
	}

	if err := h.db.QueryRow(ctx,
		`SELECT count(*) FROM orphan_pod WHERE NOT resolved`).Scan(&s.OrphanPods); err != nil {
		return s, err
	}

	ub, err := (&Biller{db: h.db}).Unbilled(ctx, now.Add(-unbilledWindow), now)
	if err != nil {
		return s, err
	}
	s.UnbilledGaps = len(ub)
	for _, u := range ub {
		s.UnbilledGPUHours += u.GPUHours
	}

	return s, nil
}
