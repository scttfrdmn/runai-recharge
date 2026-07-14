package runai

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

// roundTripFunc lets a test stand in for the network at the seam below
// ListWorkloads -- c.http.Do -- so the tripwire (pure Go logic we control) is
// pinned WITHOUT a live cluster and WITHOUT depending on which pagination scheme
// the cluster actually wants. That scheme question is issue #2's remaining half
// and genuinely needs hardware; this file is the half that does not.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// newTestClient returns a Client whose transport is fake and whose token is
// pre-seeded so bearer() never makes a network call.
func newTestClient(rt roundTripFunc) *Client {
	c := New("https://cluster.invalid", "app", "secret")
	c.http.Transport = rt
	c.token = "test-token"
	c.expires = time.Now().Add(time.Hour)
	return c
}

func jsonResponse(v any) *http.Response {
	b, _ := json.Marshal(v)
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(string(b))),
		Header:     http.Header{"Content-Type": {"application/json"}},
	}
}

func window() (time.Time, time.Time) {
	since := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	return since, since.Add(24 * time.Hour)
}

// The tripwire itself: a server that ignores our paging parameters and re-serves
// the same full page forever. The old loop (break on empty NextPageToken) would
// have looped here; the point of the fix is that being wrong is LOUD. It must
// error, and it must not hang.
func TestListWorkloads_StuckPaginationErrors(t *testing.T) {
	page := make([]Workload, workloadsPageLimit)
	for i := range page {
		page[i] = Workload{ID: "w" + strconv.Itoa(i)} // SAME ids every call
	}

	calls := 0
	c := newTestClient(func(*http.Request) (*http.Response, error) {
		calls++
		if calls > 100 {
			// A real infinite loop would never reach the assertion below; this
			// bounds the test so a regression fails loudly instead of hanging CI.
			t.Fatalf("ListWorkloads did not stop on a stuck server (%d calls) -- tripwire is blind", calls)
		}
		return jsonResponse(workloadsPage{Workloads: page}), nil
	})

	since, until := window()
	_, err := c.ListWorkloads(context.Background(), since, until)
	if err == nil {
		t.Fatal("expected an error on a full page with no new workloads, got nil")
	}
	if !strings.Contains(err.Error(), "pagination") {
		t.Fatalf("error should name the pagination fault, got: %v", err)
	}
	if calls != 2 {
		// Page 1 fills `seen`; page 2 is full with zero fresh rows -> trip.
		t.Fatalf("expected to trip on the second page, made %d calls", calls)
	}
}

// The other half of the invariant: a server that DOES advance across the
// page-limit boundary must be consumed in full, deduped, with no false trip.
func TestListWorkloads_CorrectPaginationReturnsAll(t *testing.T) {
	const total = workloadsPageLimit + 37 // forces a short final page

	c := newTestClient(func(r *http.Request) (*http.Response, error) {
		off, _ := strconv.Atoi(r.URL.Query().Get("offset"))
		var pg []Workload
		for i := off; i < total && i < off+workloadsPageLimit; i++ {
			pg = append(pg, Workload{ID: "w" + strconv.Itoa(i)})
		}
		return jsonResponse(workloadsPage{Workloads: pg}), nil
	})

	since, until := window()
	ws, err := c.ListWorkloads(context.Background(), since, until)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ws) != total {
		t.Fatalf("expected %d workloads across pages, got %d", total, len(ws))
	}
	seen := map[string]bool{}
	for _, w := range ws {
		if seen[w.ID] {
			t.Fatalf("workload %s returned twice -- dedup failed", w.ID)
		}
		seen[w.ID] = true
	}
}

// Overlapping full pages (offset advances but pages share rows) still terminate
// correctly: dedup is what makes progress measurable, so the run must complete
// without tripping as long as SOME row is new each page.
func TestListWorkloads_OverlappingPagesDedup(t *testing.T) {
	const total = workloadsPageLimit + 50
	const stride = workloadsPageLimit / 2 // each page overlaps the previous by half

	c := newTestClient(func(r *http.Request) (*http.Response, error) {
		off, _ := strconv.Atoi(r.URL.Query().Get("offset"))
		// Server advances by len(prev page)=limit, but we simulate a server that
		// only moves by `stride`, re-sending half the prior page each time.
		start := (off / workloadsPageLimit) * stride
		var pg []Workload
		for i := start; i < total && i < start+workloadsPageLimit; i++ {
			pg = append(pg, Workload{ID: "w" + strconv.Itoa(i)})
		}
		return jsonResponse(workloadsPage{Workloads: pg}), nil
	})

	since, until := window()
	ws, err := c.ListWorkloads(context.Background(), since, until)
	if err != nil {
		t.Fatalf("unexpected error on overlapping pages: %v", err)
	}
	if len(ws) != total {
		t.Fatalf("expected %d deduped workloads, got %d", total, len(ws))
	}
}

func TestListWorkloads_RejectsEmptyWindow(t *testing.T) {
	c := newTestClient(func(*http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("transport must not be called for an empty window")
	})
	now := time.Now()
	if _, err := c.ListWorkloads(context.Background(), now, now); err == nil {
		t.Fatal("expected an error for an empty [since, until) window")
	}
	if _, err := c.ListWorkloads(context.Background(), now, now.Add(-time.Hour)); err == nil {
		t.Fatal("expected an error for an inverted window")
	}
}
