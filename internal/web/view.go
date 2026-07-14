package web

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/scttfrdmn/runai-recharge/internal/bill"
)

type lineView struct {
	DateStr       string
	Submitter     string
	Workload      string
	Type          string
	Description   string
	NodePool      string
	GPUAllocStr   string
	GPUSecondsStr string
	RateStr       string
	AmountStr     string
	RunningStr    string

	UtilStr   string
	UtilClass string
	UtilWidth string
}

// sectionView is one cost basis. On-prem and AWS are separate sections with
// separate subtotals — not one blended number that hides where the money went.
type sectionView struct {
	ClassName     string
	Rates         []string
	Lines         []lineView
	GPUSecondsStr string
	SubtotalStr   string
}

type statementView struct {
	GroupName   string
	PeriodLabel string
	FundCode    string
	ScopeName   string // set only when scoped to a single class
	Provisional bool

	Sections []sectionView

	TotalStr     string
	FiscalYTDStr string
}

var errBadPeriod = errors.New("unknown period")

func view(st *bill.Statement, label string) statementView {
	v := statementView{
		GroupName:    st.GroupName,
		PeriodLabel:  label,
		ScopeName:    st.ClassName,
		Provisional:  st.Provisional,
		TotalStr:     money(st.Total),
		FiscalYTDStr: money(st.FiscalYTD),
	}

	for _, l := range st.Lines {
		if l.FundCode != "" {
			v.FundCode = l.FundCode
			break
		}
	}

	for _, sec := range st.Sections {
		sv := sectionView{
			ClassName:     nameOr(sec.ClassName, sec.Class),
			Rates:         sec.Rates,
			GPUSecondsStr: commas(sec.GPUSeconds),
			SubtotalStr:   money(sec.Subtotal),
		}
		for _, l := range sec.Lines {
			sv.Lines = append(sv.Lines, line(l))
		}
		v.Sections = append(v.Sections, sv)
	}

	return v
}

func nameOr(name, fallback string) string {
	if name != "" {
		return name
	}
	if fallback == "" {
		return "Unclassified capacity" // pool_class has a gap. Fix the mapping.
	}
	return fallback
}

func line(l bill.Line) lineView {
	lv := lineView{
		DateStr:       l.Date.Format("2006-01-02"),
		Submitter:     l.Submitter,
		Workload:      l.Workload,
		Type:          l.Type,
		Description:   l.Description,
		NodePool:      l.NodePool,
		GPUAllocStr:   strconv.FormatFloat(l.GPUAlloc, 'f', -1, 64),
		GPUSecondsStr: commas(l.GPUSeconds),
		RateStr:       "$" + strconv.FormatFloat(l.Rate, 'f', 2, 64),
		AmountStr:     money(l.Amount),
		RunningStr:    money(l.Running),
	}

	// The utilization column. Not billed. Present because it is the only line
	// item in the whole system that changes behavior.
	if l.UtilMean == nil {
		lv.UtilStr, lv.UtilClass, lv.UtilWidth = "—", "u-none", "0"
		return lv
	}

	u := *l.UtilMean
	lv.UtilStr = strconv.FormatFloat(u, 'f', 0, 64) + "%"
	lv.UtilWidth = strconv.FormatFloat(clamp(u, 0, 100), 'f', 0, 64)
	switch {
	case u >= 60:
		lv.UtilClass = "u-good"
	case u >= 25:
		lv.UtilClass = "u-mid"
	default:
		lv.UtilClass = "u-poor"
	}
	return lv
}

func clamp(x, lo, hi float64) float64 {
	if x < lo {
		return lo
	}
	if x > hi {
		return hi
	}
	return x
}

func money(f float64) string {
	s := strconv.FormatFloat(f, 'f', 2, 64)
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")

	dot := strings.IndexByte(s, '.')
	intPart, frac := s[:dot], s[dot:]

	var b strings.Builder
	for i, c := range intPart {
		if i > 0 && (len(intPart)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(c)
	}
	out := "$" + b.String() + frac
	if neg {
		out = "-" + out
	}
	return out
}

func commas(f float64) string {
	s := strconv.FormatFloat(f, 'f', 0, 64)
	var b strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(c)
	}
	return b.String()
}

// parsePeriod accepts 2026-Q3 (fiscal quarters keyed to a July 1 FY start) or
// 2026-04 (calendar month).
func parsePeriod(s string) (from, to time.Time, label string, err error) {
	if s == "" {
		now := time.Now().UTC()
		from = time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
		return from, from.AddDate(0, 1, 0), from.Format("January 2006"), nil
	}

	if i := strings.Index(s, "-Q"); i > 0 {
		y, e1 := strconv.Atoi(s[:i])
		q, e2 := strconv.Atoi(s[i+2:])
		if e1 != nil || e2 != nil || q < 1 || q > 4 {
			return from, to, "", errBadPeriod
		}
		// FY starts July 1. FY26 Q1 = Jul-Sep 2025.
		startMonth := time.Month(7 + 3*(q-1))
		startYear := y - 1
		if startMonth > 12 {
			startMonth -= 12
			startYear = y
		}
		from = time.Date(startYear, startMonth, 1, 0, 0, 0, 0, time.UTC)
		to = from.AddDate(0, 3, 0)
		return from, to, "FY" + strconv.Itoa(y%100) + " Q" + strconv.Itoa(q), nil
	}

	t, e := time.Parse("2006-01", s)
	if e != nil {
		return from, to, "", e
	}
	return t, t.AddDate(0, 1, 0), t.Format("January 2006"), nil
}
