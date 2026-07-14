package web

import (
	"encoding/csv"
	"fmt"
	"net/http"
	"strconv"

	"github.com/scttfrdmn/runai-recharge/internal/bill"
)

type reconRow struct {
	ClassName    string
	Unmapped     bool
	ClusterID    string
	NodePool     string
	GPUModel     string
	CapacityStr  string
	AllocatedStr string
	IdleStr      string
	UnbilledStr  string
	HasUnbilled  bool
	BilledStr    string
	UtilStr      string
	UtilWidth    string
	LowUtil      bool
}

type unbilledView struct {
	HoursStr  string
	ClusterID string
	NodePool  string
	GPUModel  string
	Reason    string
}

type orphanView struct {
	PodName      string
	NodeName     string
	WorkloadName string
	Submitter    string
	Project      string
	StartedStr   string
}

type reconView struct {
	PeriodLabel string

	CapacityStr  string
	IdleStr      string
	IdlePctStr   string
	AllocatedStr string
	UnbilledStr  string
	BilledStr    string

	Clean          bool
	ExceptionCount int

	Rows     []reconRow
	Unbilled []unbilledView
	Orphans  []orphanView
}

func (s *Server) reconciliation(w http.ResponseWriter, r *http.Request) {
	// Reconciliation is an operator document: it spans every group and every
	// cost basis. Admin only, and not by convention -- by predicate.
	_, admin, err := s.Authz(r)
	if err != nil || !admin {
		http.NotFound(w, r)
		return
	}

	from, to, label, err := parsePeriod(r.URL.Query().Get("period"))
	if err != nil {
		http.Error(w, "Unknown period. Use 2026-Q3 or 2026-04.", http.StatusBadRequest)
		return
	}

	rec, err := s.recon.Run(r.Context(), from, to)
	if err != nil {
		http.Error(w, "Could not build the reconciliation.", http.StatusInternalServerError)
		return
	}

	if r.URL.Query().Get("format") == "csv" {
		reconCSV(w, rec, label)
		return
	}

	_ = s.tmpl.ExecuteTemplate(w, "reconciliation.html", reconViewOf(rec, label))
}

func reconViewOf(r *bill.Reconciliation, label string) reconView {
	v := reconView{
		PeriodLabel:    label,
		CapacityStr:    hours(r.TotalCapacityHours),
		IdleStr:        hours(r.TotalIdleHours),
		AllocatedStr:   hours(r.TotalAllocatedHours),
		UnbilledStr:    hours(r.TotalUnbilledHours),
		BilledStr:      money(r.TotalBilled),
		Clean:          r.Clean(),
		ExceptionCount: len(r.Unbilled) + len(r.Orphans),
	}

	if r.TotalCapacityHours > 0 {
		pct := r.TotalIdleHours / r.TotalCapacityHours * 100
		v.IdlePctStr = strconv.FormatFloat(pct, 'f', 0, 64) + "%"
	} else {
		v.IdlePctStr = "—"
	}

	for _, c := range r.Capacities {
		u := c.Utilization() * 100
		row := reconRow{
			ClassName:    c.ClassName,
			Unmapped:     c.Class == "",
			ClusterID:    c.ClusterID,
			NodePool:     c.NodePool,
			GPUModel:     c.GPUModel,
			CapacityStr:  hours(c.CapacityHours),
			AllocatedStr: hours(c.AllocatedHours),
			IdleStr:      hours(c.IdleHours()),
			UnbilledStr:  hours(c.UnbilledHours()),
			HasUnbilled:  c.UnbilledHours() > 0.05,
			BilledStr:    money(c.Billed),
			UtilStr:      strconv.FormatFloat(u, 'f', 0, 64) + "%",
			UtilWidth:    strconv.FormatFloat(clamp(u, 0, 100), 'f', 0, 64),
			LowUtil:      u < 50,
		}
		v.Rows = append(v.Rows, row)
	}

	for _, u := range r.Unbilled {
		v.Unbilled = append(v.Unbilled, unbilledView{
			HoursStr:  hours(u.GPUHours),
			ClusterID: u.ClusterID,
			NodePool:  u.NodePool,
			GPUModel:  u.GPUModel,
			Reason:    u.Reason,
		})
	}

	for _, o := range r.Orphans {
		v.Orphans = append(v.Orphans, orphanView{
			PodName:      o.PodName,
			NodeName:     o.NodeName,
			WorkloadName: o.WorkloadName,
			Submitter:    o.Submitter,
			Project:      o.Project,
			StartedStr:   o.StartedAt.Format("2006-01-02 15:04"),
		})
	}

	return v
}

func reconCSV(w http.ResponseWriter, r *bill.Reconciliation, label string) {
	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition",
		fmt.Sprintf(`attachment; filename="reconciliation-%s.csv"`, label))

	c := csv.NewWriter(w)
	defer c.Flush()

	_ = c.Write([]string{"rate_class", "cluster_id", "node_pool", "gpu_model",
		"capacity_gpu_hours", "allocated_gpu_hours", "idle_gpu_hours",
		"unbilled_gpu_hours", "billed_usd", "utilization_pct"})

	for _, x := range r.Capacities {
		_ = c.Write([]string{
			x.Class, x.ClusterID, x.NodePool, x.GPUModel,
			f1(x.CapacityHours), f1(x.AllocatedHours), f1(x.IdleHours()),
			f1(x.UnbilledHours()), strconv.FormatFloat(x.Billed, 'f', 2, 64),
			f1(x.Utilization() * 100),
		})
	}
}

func f1(f float64) string { return strconv.FormatFloat(f, 'f', 1, 64) }

func hours(f float64) string {
	if f < 0 {
		f = 0 // capacity sampling can undershoot allocation by a poll interval
	}
	return commas(f)
}
