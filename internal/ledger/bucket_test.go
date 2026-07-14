package ledger

import (
	"math"
	"testing"
	"time"
)

func ts(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

func approx(a, b float64) bool { return math.Abs(a-b) < 1e-6 }

func TestSliceWithinSingleHour(t *testing.T) {
	bs := Slice(ts("2026-04-02T10:15:00Z"), ts("2026-04-02T10:45:30Z"), 8)
	if len(bs) != 1 {
		t.Fatalf("want 1 bucket, got %d", len(bs))
	}
	if !bs[0].HourStart.Equal(ts("2026-04-02T10:00:00Z")) {
		t.Fatalf("bad hour start: %v", bs[0].HourStart)
	}
	if !approx(bs[0].Seconds, 1830) {
		t.Fatalf("want 1830s, got %v", bs[0].Seconds)
	}
	if !approx(bs[0].GPUSeconds, 1830*8) {
		t.Fatalf("want %v gpu-sec, got %v", 1830*8, bs[0].GPUSeconds)
	}
}

func TestSliceCrossesHourBoundary(t *testing.T) {
	// 10:45 -> 11:20 = 15 min in hour 10, 20 min in hour 11
	bs := Slice(ts("2026-04-02T10:45:00Z"), ts("2026-04-02T11:20:00Z"), 1)
	if len(bs) != 2 {
		t.Fatalf("want 2 buckets, got %d", len(bs))
	}
	if !approx(bs[0].Seconds, 900) || !approx(bs[1].Seconds, 1200) {
		t.Fatalf("bad split: %v %v", bs[0].Seconds, bs[1].Seconds)
	}
	if !bs[1].HourStart.Equal(ts("2026-04-02T11:00:00Z")) {
		t.Fatalf("bad second hour: %v", bs[1].HourStart)
	}
}

func TestSliceExactHourAlignment(t *testing.T) {
	// Exactly two whole hours must produce exactly two buckets, not three.
	bs := Slice(ts("2026-04-02T10:00:00Z"), ts("2026-04-02T12:00:00Z"), 1)
	if len(bs) != 2 {
		t.Fatalf("want 2 buckets, got %d", len(bs))
	}
	for i, b := range bs {
		if !approx(b.Seconds, 3600) {
			t.Fatalf("bucket %d: want 3600s, got %v", i, b.Seconds)
		}
	}
}

func TestSliceLongRunningJobConservesMass(t *testing.T) {
	// A six-week job. Total GPU-seconds must equal duration * alloc exactly.
	start := ts("2026-04-02T13:37:11Z")
	end := start.Add(42 * 24 * time.Hour).Add(17 * time.Minute).Add(3 * time.Second)
	const alloc = 8

	bs := Slice(start, end, alloc)
	want := end.Sub(start).Seconds() * alloc

	if got := TotalGPUSeconds(bs); !approx(got, want) {
		t.Fatalf("mass not conserved: want %v, got %v (delta %v)", want, got, got-want)
	}
	// Sanity: 42 days + change spans 1010 hour buckets.
	if len(bs) < 1000 {
		t.Fatalf("suspiciously few buckets: %d", len(bs))
	}
}

func TestSliceFractionalGPU(t *testing.T) {
	// Run:ai fractional allocation. No special case — just multiplication.
	bs := Slice(ts("2026-04-02T10:00:00Z"), ts("2026-04-02T11:00:00Z"), 0.5)
	if !approx(bs[0].GPUSeconds, 1800) {
		t.Fatalf("want 1800 gpu-sec for 0.5 GPU over 1h, got %v", bs[0].GPUSeconds)
	}
}

func TestSliceDegenerate(t *testing.T) {
	x := ts("2026-04-02T10:00:00Z")
	if bs := Slice(x, x, 8); bs != nil {
		t.Fatalf("zero-length interval must produce no buckets, got %d", len(bs))
	}
	if bs := Slice(x, x.Add(-time.Hour), 8); bs != nil {
		t.Fatalf("inverted interval must produce no buckets, got %d", len(bs))
	}
}

func TestSliceRunningJobConvergesOnUpsert(t *testing.T) {
	// Poll at T+30m, then again at T+90m. The open hour is rewritten, not
	// double-counted, because the store keys on (workload, model, hour_start).
	start := ts("2026-04-02T10:00:00Z")

	first := Slice(start, start.Add(30*time.Minute), 1)
	second := Slice(start, start.Add(90*time.Minute), 1)

	// The hour-10 bucket must be a REPLACEMENT (1800 -> 3600), not an addition.
	if !approx(first[0].Seconds, 1800) {
		t.Fatalf("first poll: want 1800, got %v", first[0].Seconds)
	}
	if !approx(second[0].Seconds, 3600) {
		t.Fatalf("second poll hour-10: want 3600, got %v", second[0].Seconds)
	}
	if !approx(second[1].Seconds, 1800) {
		t.Fatalf("second poll hour-11: want 1800, got %v", second[1].Seconds)
	}
}
