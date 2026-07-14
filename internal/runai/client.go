// Package runai is a thin client for the NVIDIA Run:ai control-plane REST API.
//
// From cluster version 2.17 onward the control-plane API is the supported path
// for metrics; direct Prometheus scraping is deprecated. Do not build on
// Prometheus.
//
// !! VERIFY BEFORE TRUSTING A SINGLE NUMBER !!
//
// Field names and, more importantly, the aggregation semantics of
// GPU_ALLOCATION for DISTRIBUTED workloads vary by cluster version. Run:ai
// aggregates metrics at the workload level across worker pods. Whether
// GPU_ALLOCATION comes back SUMMED across workers or PER-POD determines whether
// a 4-worker x 8-GPU job bills as 32 GPUs or as 8.
//
// Submit one known-shape distributed job. Read it back. Check the arithmetic.
// Do this on day one. It is exactly the kind of thing that produces a statement
// that is off by 4x and goes unnoticed for two quarters.
//
// `make verify-alloc` exists for this.
package runai

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

type Client struct {
	base   string
	appID  string
	secret string
	http   *http.Client

	mu      sync.Mutex
	token   string
	expires time.Time
}

func New(base, appID, secret string) *Client {
	return &Client{
		base:   base,
		appID:  appID,
		secret: secret,
		http:   &http.Client{Timeout: 60 * time.Second},
	}
}

// ---------------------------------------------------------------------------
// auth
// ---------------------------------------------------------------------------

type tokenResp struct {
	AccessToken string `json:"accessToken"`
	ExpiresIn   int    `json:"expiresIn"`
}

func (c *Client) bearer(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.token != "" && time.Now().Before(c.expires.Add(-30*time.Second)) {
		return c.token, nil
	}

	body, _ := json.Marshal(map[string]string{
		"grantType": "app_token",
		"appId":     c.appID,
		"appSecret": c.secret,
	})

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.base+"/api/v1/token", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("runai: token request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("runai: token: status %d", resp.StatusCode)
	}

	var tr tokenResp
	if err := json.NewDecoder(resp.Body).Decode(&tr); err != nil {
		return "", err
	}

	c.token = tr.AccessToken
	c.expires = time.Now().Add(time.Duration(tr.ExpiresIn) * time.Second)
	return c.token, nil
}

func (c *Client) get(ctx context.Context, path string, q url.Values, out any) error {
	tok, err := c.bearer(ctx)
	if err != nil {
		return err
	}

	u := c.base + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("runai: GET %s: %w", path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("runai: GET %s: status %d", path, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// ---------------------------------------------------------------------------
// workloads
// ---------------------------------------------------------------------------

// Workload is the identity record. This is the source of truth for WHO and
// WHEN. Timestamps here are exact; the metrics series is not.
type Workload struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Type      string `json:"type"`
	Phase     string `json:"phase"`
	ClusterID string `json:"clusterId"`

	// Identity. `submittedBy` is the SSO subject.
	SubmittedBy    string `json:"submittedBy"`
	ProjectName    string `json:"projectName"`
	DepartmentName string `json:"departmentName"`
	Namespace      string `json:"namespace"`

	CreatedAt   time.Time  `json:"createdAt"`
	StartedAt   *time.Time `json:"runningAt"`
	CompletedAt *time.Time `json:"completedAt"`

	Spec WorkloadSpec `json:"spec"`

	// Annotations carry the fields you enforce at submission time via Run:ai
	// policy: recharge.fund-code, recharge.description. Without these, "what
	// they ran" is `test2` and `jobs-final-FINAL`, and the research office
	// learns nothing.
	Annotations map[string]string `json:"annotations"`
	Labels      map[string]string `json:"labels"`
}

type WorkloadSpec struct {
	Image string `json:"image"`
	// GPUDevicesRequest is whole GPUs per pod; GPUPortionRequest is the
	// fractional-GPU knob (0.5, 0.25, ...). Exactly one is meaningful.
	GPUDevicesRequest *int     `json:"gpuDevicesRequest"`
	GPUPortionRequest *float64 `json:"gpuPortionRequest"`
	NodePools         []string `json:"nodePools"`
}

// GPUAlloc collapses the two request shapes into a single fractional number.
// Whole-GPU and fractional-GPU are the same arithmetic downstream.
func (s WorkloadSpec) GPUAlloc() float64 {
	if s.GPUPortionRequest != nil && *s.GPUPortionRequest > 0 {
		return *s.GPUPortionRequest
	}
	if s.GPUDevicesRequest != nil {
		return float64(*s.GPUDevicesRequest)
	}
	return 0
}

type workloadsPage struct {
	Workloads     []Workload `json:"workloads"`
	NextPageToken string     `json:"nextPageToken"`
}

// ListWorkloads returns every workload that was updated in [since, until).
//
// Poll on a cron and PERSIST. The metrics API is retention-limited and workload
// records age out. People try to backfill a quarter and discover the data is
// gone. Build the ledger first; worry about the web page second.
func (c *Client) ListWorkloads(ctx context.Context, since, until time.Time) ([]Workload, error) {
	var all []Workload
	page := ""

	for {
		q := url.Values{}
		q.Set("filterBy", "updatedAt>="+since.UTC().Format(time.RFC3339))
		q.Set("limit", "200")
		if page != "" {
			q.Set("offset", page)
		}

		var p workloadsPage
		if err := c.get(ctx, "/api/v1/workloads", q, &p); err != nil {
			return nil, err
		}
		all = append(all, p.Workloads...)

		if p.NextPageToken == "" {
			break
		}
		page = p.NextPageToken
	}
	return all, nil
}

// ---------------------------------------------------------------------------
// metrics — used ONLY for utilization
// ---------------------------------------------------------------------------

type MetricType string

const (
	// The only metric we pull. Reported on the statement, never billed.
	// Reserving a GPU and idling it at 3% is the user's problem, not the
	// center's — but they should have to look at the number.
	GPUUtilization MetricType = "GPU_UTILIZATION"
)

type metricsResp struct {
	Measurements []struct {
		Type   string `json:"type"`
		Values []struct {
			Timestamp time.Time `json:"timestamp"`
			Value     string    `json:"value"`
		} `json:"values"`
	} `json:"measurements"`
}

// UtilizationByHour returns mean GPU utilization (0-100) keyed by UTC hour.
//
// This is the ONLY place the sampled metric series is used. It is not used to
// derive duration or GPU-seconds — those come analytically from the workload's
// start/stop timestamps, which are exact.
func (c *Client) UtilizationByHour(ctx context.Context, workloadID string, from, to time.Time) (map[time.Time]float64, error) {
	span := to.Sub(from)
	samples := int(span/(5*time.Minute)) + 1
	if samples < 2 {
		samples = 2
	}
	if samples > 1000 {
		samples = 1000
	}

	q := url.Values{}
	q.Set("metricType", string(GPUUtilization))
	q.Set("start", from.UTC().Format(time.RFC3339))
	q.Set("end", to.UTC().Format(time.RFC3339))
	q.Set("numberOfSamples", strconv.Itoa(samples))

	var mr metricsResp
	err := c.get(ctx, "/api/v1/workloads/"+workloadID+"/metrics", q, &mr)
	if err != nil {
		// Utilization is decoration. A missing series must never block billing.
		return nil, err
	}

	type acc struct {
		sum float64
		n   int
	}
	byHour := map[time.Time]*acc{}

	for _, m := range mr.Measurements {
		for _, v := range m.Values {
			f, err := strconv.ParseFloat(v.Value, 64)
			if err != nil {
				continue
			}
			h := v.Timestamp.UTC().Truncate(time.Hour)
			a, ok := byHour[h]
			if !ok {
				a = &acc{}
				byHour[h] = a
			}
			a.sum += f
			a.n++
		}
	}

	out := make(map[time.Time]float64, len(byHour))
	for h, a := range byHour {
		if a.n > 0 {
			out[h] = a.sum / float64(a.n)
		}
	}
	return out, nil
}
