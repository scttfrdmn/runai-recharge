// Package ledger converts workload intervals into hourly usage buckets.
//
// The grain of the ledger is (workload_id, gpu_model, hour_start). Everything
// downstream — mid-period rate changes, month boundaries, partial-month
// statements, re-running a closed statement — falls out of that grain for free.
//
// GPU-seconds are computed ANALYTICALLY from the workload's start/stop
// timestamps, never by integrating a sampled metric series. The metrics API is
// sampled; integrating it introduces an error of up to one sample interval on
// every job and produces gaps when the collector misses a scrape. The workload
// record's timestamps are exact. Utilization is the only thing we take from the
// metric series, and utilization is never billed.
package ledger

import (
	"time"
)

// Bucket is one workload's usage within one clock hour.
type Bucket struct {
	// HourStart is the UTC hour boundary this bucket belongs to.
	HourStart time.Time

	// Seconds is wall-clock time the workload occupied within this hour.
	Seconds float64

	// GPUSeconds is Seconds * GPUAlloc. This is the billable quantity.
	// It is fractional whenever GPUAlloc is fractional (Run:ai fractional GPU),
	// and there is no special case for that: it is multiplication.
	GPUSeconds float64
}

// Slice divides the half-open interval [start, end) into hourly buckets and
// multiplies each by gpuAlloc.
//
// For a workload that is still running, the caller passes the current time as
// end. Because the store upserts idempotently on (workload_id, gpu_model,
// hour_start), re-slicing a running workload on each poll simply overwrites the
// open hour with a longer value until the workload completes and the final
// timestamps are known. No special handling for in-flight jobs is required.
//
// Returns nil for a zero-length or inverted interval.
func Slice(start, end time.Time, gpuAlloc float64) []Bucket {
	start = start.UTC()
	end = end.UTC()

	if !end.After(start) {
		return nil
	}

	var out []Bucket

	// Walk hour boundaries. cur is the start of the segment we're about to emit.
	for cur := start; cur.Before(end); {
		hourStart := cur.Truncate(time.Hour)
		nextHour := hourStart.Add(time.Hour)

		// The segment ends at whichever comes first: the next hour boundary,
		// or the end of the workload.
		segEnd := nextHour
		if end.Before(segEnd) {
			segEnd = end
		}

		secs := segEnd.Sub(cur).Seconds()
		out = append(out, Bucket{
			HourStart:  hourStart,
			Seconds:    secs,
			GPUSeconds: secs * gpuAlloc,
		})

		cur = segEnd
	}

	return out
}

// TotalGPUSeconds is a convenience for asserting that a slice conserves mass.
func TotalGPUSeconds(bs []Bucket) float64 {
	var t float64
	for _, b := range bs {
		t += b.GPUSeconds
	}
	return t
}
