package web

import (
	"fmt"
	"io"
	"time"

	"github.com/scttfrdmn/runai-recharge/internal/bill"
)

// RenderDemo writes a sample statement. No database, no cluster.
//
// The sample deliberately shows a group that ran in BOTH capacities in one
// month — which is the case the whole design exists for. Two cost bases, two
// rates, two subtotals, one grand total.
func RenderDemo(w io.Writer) error {
	s, err := NewServer(nil, nil)
	if err != nil {
		return err
	}

	u := func(f float64) *float64 { return &f }
	d := func(s string) time.Time {
		t, _ := time.Parse(time.RFC3339, s)
		return t
	}

	const (
		onpremRate = 1.05 // FY24 capital, yr 3 of 5
		awsRate    = 2.40 // FY27 Capacity Block
	)

	lines := []bill.Line{
		// --- on-prem: owned, depreciating, cheap -----------------------------
		{Date: d("2026-04-02T09:14:00Z"), Submitter: "jchen", Workload: "protein-fold-3",
			Type: "Training", Description: "AlphaFold3 ensemble, PCSK9 variants",
			FundCode: "R01-GM-114455", Class: "onprem", ClassName: "On-premises capacity",
			NodePool: "default", GPUModel: "H100-80GB", GPUAlloc: 8,
			GPUSeconds: 115203, UtilMean: u(91), Rate: onpremRate},
		{Date: d("2026-04-02T11:02:00Z"), Submitter: "mrivera", Workload: "dev-workspace",
			Type: "Workspace", FundCode: "R01-GM-114455",
			Class: "onprem", ClassName: "On-premises capacity",
			NodePool: "default", GPUModel: "H100-80GB", GPUAlloc: 1,
			GPUSeconds: 7200, UtilMean: u(3), Rate: onpremRate},
		{Date: d("2026-04-09T16:48:00Z"), Submitter: "kobrien", Workload: "test2",
			Type: "Training", Class: "onprem", ClassName: "On-premises capacity",
			NodePool: "default", GPUModel: "H100-80GB", GPUAlloc: 0.5,
			GPUSeconds: 41400, UtilMean: u(38), Rate: onpremRate},

		// --- AWS: burst, Capacity Block, expensive ---------------------------
		{Date: d("2026-04-05T02:31:00Z"), Submitter: "jchen", Workload: "protein-fold-4",
			Type: "Training", Description: "AlphaFold3 ensemble, rerun w/ MSA depth 512",
			FundCode: "R01-GM-114455", Class: "aws-burst", ClassName: "AWS burst capacity",
			NodePool: "aws", GPUModel: "H100-80GB", GPUAlloc: 8,
			GPUSeconds: 115203, UtilMean: u(94), Rate: awsRate},
		{Date: d("2026-04-14T08:00:00Z"), Submitter: "mrivera", Workload: "seq-embed-batch",
			Type: "Inference", Description: "ESM-2 embeddings, 1.2M sequences",
			FundCode: "R01-GM-114455", Class: "aws-burst", ClassName: "AWS burst capacity",
			NodePool: "aws", GPUModel: "H100-80GB", GPUAlloc: 4,
			GPUSeconds: 208616, UtilMean: u(77), Rate: awsRate},
		{Date: d("2026-04-21T22:10:00Z"), Submitter: "kobrien", Workload: "jobs-final-FINAL",
			Type: "Training", Class: "aws-burst", ClassName: "AWS burst capacity",
			NodePool: "aws", GPUModel: "H100-80GB", GPUAlloc: 8,
			GPUSeconds: 61200, UtilMean: nil, Rate: awsRate},
	}

	sections := map[string]*bill.Section{}
	var order []string
	var running, total float64

	for i := range lines {
		l := &lines[i]
		l.Amount = l.GPUSeconds * l.Rate / 3600.0
		running += l.Amount
		l.Running = running
		total += l.Amount

		sec, ok := sections[l.Class]
		if !ok {
			sec = &bill.Section{Class: l.Class, ClassName: l.ClassName,
				Rates: []string{fmt.Sprintf("%s $%.2f/GPU-hr", l.GPUModel, l.Rate)}}
			sections[l.Class] = sec
			order = append(order, l.Class)
		}
		sec.Lines = append(sec.Lines, *l)
		sec.GPUSeconds += l.GPUSeconds
		sec.Subtotal += l.Amount
	}

	st := &bill.Statement{
		GroupID:     "chen-lab",
		GroupName:   "Neuroscience / Chen Lab",
		PeriodStart: d("2026-04-01T00:00:00Z"),
		PeriodEnd:   d("2026-05-01T00:00:00Z"),
		Provisional: true,
		Lines:       lines,
		Total:       total,
		FiscalYTD:   total + 11204.55,
	}
	for _, c := range order {
		st.Sections = append(st.Sections, *sections[c])
	}

	return s.tmpl.ExecuteTemplate(w, "statement.html", view(st, "April 2026"))
}

// RenderReconDemo writes a sample reconciliation statement. No DB, no cluster.
//
// The sample is deliberately DIRTY: an idle cloud pool, an unmapped node pool,
// and an orphaned pod. A clean example teaches you nothing -- the document
// exists for the days it isn't clean.
func RenderReconDemo(w io.Writer) error {
	s, err := NewServer(nil, nil)
	if err != nil {
		return err
	}
	d := func(s string) time.Time {
		t, _ := time.Parse(time.RFC3339, s)
		return t
	}

	caps := []bill.CapacityLine{
		{Class: "onprem", ClassName: "On-premises capacity",
			ClusterID: "onprem-cluster", NodePool: "default", GPUModel: "H100-80GB",
			CapacityHours: 23040, AllocatedHours: 16918, BilledHours: 16918,
			Billed: 17763.90},

		// The idle cloud pool. Nobody's fault, and the reason it's on the page.
		{Class: "aws-burst", ClassName: "AWS burst capacity",
			ClusterID: "onprem-cluster", NodePool: "aws", GPUModel: "H100-80GB",
			CapacityHours: 5760, AllocatedHours: 1846, BilledHours: 1846,
			Billed: 4430.40},

		// The bug: a pool nobody mapped. Real usage, on no statement.
		{Class: "", ClassName: "UNMAPPED",
			ClusterID: "onprem-cluster", NodePool: "inference", GPUModel: "L40S",
			CapacityHours: 1440, AllocatedHours: 312, BilledHours: 0, Billed: 0},
	}

	r := &bill.Reconciliation{
		PeriodStart: d("2026-04-01T00:00:00Z"),
		PeriodEnd:   d("2026-05-01T00:00:00Z"),
		Capacities:  caps,
		Unbilled: []bill.Unbilled{
			{ClusterID: "onprem-cluster", NodePool: "inference", GPUModel: "L40S",
				Class: "", GPUHours: 312.4,
				Reason: "no pool_class mapping for this cluster/pool"},
		},
		Orphans: []bill.Orphan{
			{WorkloadID: "wl-8842", WorkloadName: "esm-sweep-7", PodName: "esm-sweep-7-0",
				NodeName: "ip-10-3-14-201.ec2.internal", Submitter: "kobrien",
				Project: "neuro-chen", StartedAt: d("2026-04-18T03:22:00Z")},
		},
	}
	for _, c := range caps {
		r.TotalCapacityHours += c.CapacityHours
		r.TotalAllocatedHours += c.AllocatedHours
		r.TotalUnbilledHours += c.UnbilledHours()
		r.TotalBilled += c.Billed
	}
	r.TotalIdleHours = r.TotalCapacityHours - r.TotalAllocatedHours

	return s.tmpl.ExecuteTemplate(w, "reconciliation.html", reconViewOf(r, "April 2026"))
}
