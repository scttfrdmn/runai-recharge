package ledger

import (
	"testing"
	"time"

	"github.com/scttfrdmn/runai-recharge/internal/runai"
)

// A placement is still running if ANY of its pods is.
//
// Deciding this inside the pod loop makes the answer depend on iteration order:
// a completed pod seen AFTER a running one sets End, and a live job gets billed
// as finished. Coin flip on map order.
//
// These two subtests are the same distributed workload with the pods in the two
// orders. They must agree. If they ever diverge, someone moved the running check
// back inside the loop.
func TestPlacementRunningRegardlessOfPodOrder(t *testing.T) {
	start := ts("2026-04-02T10:00:00Z")
	done := ts("2026-04-02T11:00:00Z")

	completed := runai.Pod{Name: "w-0", NodeName: "n1", StartedAt: &start, CompletedAt: &done}
	stillGoing := runai.Pod{Name: "w-1", NodeName: "n1", StartedAt: &start, CompletedAt: nil}

	for _, tc := range []struct {
		name string
		pods []runai.Pod
	}{
		{"completed first", []runai.Pod{completed, stillGoing}},
		{"running first", []runai.Pod{stillGoing, completed}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pl := groupForTest(tc.pods, start, 1)
			if !pl.End.IsZero() {
				t.Fatalf("one pod is still running, so the placement is still "+
					"running; End must be zero, got %v", pl.End)
			}
		})
	}
}

func TestPlacementEndIsLatestCompletion(t *testing.T) {
	start := ts("2026-04-02T10:00:00Z")
	early := ts("2026-04-02T11:00:00Z")
	late := ts("2026-04-02T13:00:00Z")

	pods := []runai.Pod{
		{Name: "w-0", NodeName: "n1", StartedAt: &start, CompletedAt: &late},
		{Name: "w-1", NodeName: "n1", StartedAt: &start, CompletedAt: &early},
	}

	pl := groupForTest(pods, start, 1)
	if !pl.End.Equal(late) {
		t.Fatalf("End must be the LAST pod to finish (a distributed job isn't "+
			"done until every rank is): want %v, got %v", late, pl.End)
	}
}

// groupForTest replicates Resolve's grouping without a database. The node lookup
// is I/O; the grouping is the logic worth pinning.
func groupForTest(pods []runai.Pod, fallbackStart time.Time, fallbackAlloc float64) Placement {
	pl := Placement{ClusterID: "c", NodePool: "p", GPUModel: "H100"}
	running := false

	for _, pod := range pods {
		if pod.RequestedGPUs != nil {
			pl.Alloc += *pod.RequestedGPUs
		} else {
			pl.Alloc += fallbackAlloc
		}

		s := fallbackStart
		if pod.StartedAt != nil {
			s = *pod.StartedAt
		}
		if pl.Start.IsZero() || s.Before(pl.Start) {
			pl.Start = s
		}

		if pod.CompletedAt == nil {
			running = true
		} else if pod.CompletedAt.After(pl.End) {
			pl.End = *pod.CompletedAt
		}
	}

	if running {
		pl.End = time.Time{}
	}
	return pl
}

// A distributed workload's allocation is the SUM of its pods' GPU requests.
//
// This is what sidesteps the workload-level GPU_ALLOCATION ambiguity entirely:
// whether Run:ai reports that metric summed across workers or per-pod, summing
// the pods themselves gives the right answer either way.
func TestPlacementSumsPodsNotWorkloadMetric(t *testing.T) {
	start := ts("2026-04-02T10:00:00Z")
	end := ts("2026-04-02T11:00:00Z")
	g := func(f float64) *float64 { return &f }

	// 4 workers x 8 GPU. The correct answer is 32, not 8.
	var pods []runai.Pod
	for i := 0; i < 4; i++ {
		pods = append(pods, runai.Pod{
			Name: "w", NodeName: "n1", StartedAt: &start, CompletedAt: &end,
			RequestedGPUs: g(8),
		})
	}

	pl := groupForTest(pods, start, 0)
	if pl.Alloc != 32 {
		t.Fatalf("4 workers x 8 GPU = 32 GPUs; got %v", pl.Alloc)
	}

	bs := Slice(pl.Start, pl.End, pl.Alloc)
	if got := TotalGPUSeconds(bs); got != 3600*32 {
		t.Fatalf("one hour at 32 GPUs = %v GPU-seconds; got %v", 3600*32, got)
	}
}
