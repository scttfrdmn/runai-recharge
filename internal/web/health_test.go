package web

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/scttfrdmn/runai-recharge/internal/bill"
)

// A fixed clock and a stubbed snapshot let us pin the health decisions -- 503 on
// a stale poll, the never-polled case, the metrics format -- WITHOUT a database.
// The staleness threshold is what makes a week-long silent poll gap page
// someone; it is load-bearing and tested directly.
func testServer(snap bill.HealthSnapshot, now time.Time) *Server {
	return &Server{
		StaleAfter:     15 * time.Minute, // 3x a 5m interval
		UnbilledWindow: 45 * 24 * time.Hour,
		now:            func() time.Time { return now },
		snapshot: func(context.Context, time.Time, time.Duration) (bill.HealthSnapshot, error) {
			return snap, nil
		},
	}
}

func get(t *testing.T, s *Server, path string) *httptest.ResponseRecorder {
	t.Helper()
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if path == "/healthz" {
		s.healthz(rr, req)
	} else {
		s.metrics(rr, req)
	}
	return rr
}

func TestHealthz(t *testing.T) {
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)

	t.Run("fresh poll is 200", func(t *testing.T) {
		s := testServer(bill.HealthSnapshot{
			EverPolled: true, LastPollSuccess: now.Add(-2 * time.Minute),
		}, now)
		rr := get(t, s, "/healthz")
		if rr.Code != http.StatusOK {
			t.Errorf("code = %d, want 200; body=%q", rr.Code, rr.Body.String())
		}
	})

	t.Run("stale poll is 503", func(t *testing.T) {
		// Past the threshold must trip: the whole point is that a stalled poll --
		// the only unrecoverable failure -- pages someone.
		s := testServer(bill.HealthSnapshot{
			EverPolled: true, LastPollSuccess: now.Add(-16 * time.Minute),
		}, now)
		rr := get(t, s, "/healthz")
		if rr.Code != http.StatusServiceUnavailable {
			t.Errorf("code = %d, want 503; body=%q", rr.Code, rr.Body.String())
		}
		if !strings.Contains(rr.Body.String(), "STALE") {
			t.Errorf("body should say STALE, got %q", rr.Body.String())
		}
	})

	t.Run("never polled is 503", func(t *testing.T) {
		// A brand-new deploy that has never completed a poll is NOT healthy:
		// nothing has proven the pipe works. Silent zero would be reporting OK.
		s := testServer(bill.HealthSnapshot{EverPolled: false}, now)
		rr := get(t, s, "/healthz")
		if rr.Code != http.StatusServiceUnavailable {
			t.Errorf("code = %d, want 503; body=%q", rr.Code, rr.Body.String())
		}
	})

	t.Run("just under threshold is 200", func(t *testing.T) {
		s := testServer(bill.HealthSnapshot{
			EverPolled: true, LastPollSuccess: now.Add(-14 * time.Minute),
		}, now)
		if rr := get(t, s, "/healthz"); rr.Code != http.StatusOK {
			t.Errorf("code = %d, want 200", rr.Code)
		}
	})
}

func TestMetrics(t *testing.T) {
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	last := now.Add(-2 * time.Minute)

	s := testServer(bill.HealthSnapshot{
		EverPolled:       true,
		LastPollSuccess:  last,
		OrphanPods:       3,
		UnbilledGPUHours: 312.5,
		UnbilledGaps:     2,
	}, now)
	rr := get(t, s, "/metrics")

	if rr.Code != http.StatusOK {
		t.Fatalf("code = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	wants := []string{
		"recharge_poll_last_success_timestamp " + strconv.FormatInt(last.Unix(), 10),
		"recharge_poll_stale 0",
		"recharge_orphan_pods 3",
		"recharge_unbilled_gpu_hours 312.5",
		"recharge_unbilled_gaps 2",
		"# TYPE recharge_poll_stale gauge",
	}
	for _, want := range wants {
		if !strings.Contains(body, want) {
			t.Errorf("metrics missing %q\n---\n%s", want, body)
		}
	}
}

func TestMetricsStaleAndNeverPolled(t *testing.T) {
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)

	t.Run("stale sets the gauge to 1", func(t *testing.T) {
		s := testServer(bill.HealthSnapshot{
			EverPolled: true, LastPollSuccess: now.Add(-time.Hour),
		}, now)
		if body := get(t, s, "/metrics").Body.String(); !strings.Contains(body, "recharge_poll_stale 1") {
			t.Errorf("expected recharge_poll_stale 1, got:\n%s", body)
		}
	})

	t.Run("never polled reports zero timestamp and stale 1", func(t *testing.T) {
		s := testServer(bill.HealthSnapshot{EverPolled: false}, now)
		body := get(t, s, "/metrics").Body.String()
		// 0 timestamp is deliberate: alerting reads it as infinitely stale rather
		// than as "just polled at the epoch."
		if !strings.Contains(body, "recharge_poll_last_success_timestamp 0") {
			t.Errorf("never-polled must emit timestamp 0, got:\n%s", body)
		}
		if !strings.Contains(body, "recharge_poll_stale 1") {
			t.Errorf("never-polled must be stale, got:\n%s", body)
		}
	})
}
