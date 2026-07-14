package runai

import (
	"context"
	"net/url"
	"time"
)

// ---------------------------------------------------------------------------
// nodes — the only thing that knows where a GPU actually is
// ---------------------------------------------------------------------------

type Node struct {
	Name      string `json:"name"`
	ClusterID string `json:"clusterId"`
	NodePool  string `json:"nodePool"`

	Status struct {
		GPUInfo struct {
			// nvidia.com/gpu.product, e.g. "NVIDIA-H100-80GB-HBM3"
			GPUType  string `json:"gpuType"`
			GPUCount int    `json:"gpuCount"`
		} `json:"gpuInfo"`
	} `json:"status"`
}

func (n Node) GPUModel() string { return n.Status.GPUInfo.GPUType }
func (n Node) GPUCount() int    { return n.Status.GPUInfo.GPUCount }

// ListNodes refreshes the node cache.
//
// Call this on EVERY poll, before ingesting workloads. In AWS, nodes are scaled
// to zero and come back with new names; a node that vanished between polls must
// still resolve for the hours it served, which is why the store keeps history
// and never deletes.
func (c *Client) ListNodes(ctx context.Context) ([]Node, error) {
	var resp struct {
		Nodes []Node `json:"nodes"`
	}
	if err := c.get(ctx, "/api/v1/nodes", nil, &resp); err != nil {
		return nil, err
	}
	return resp.Nodes, nil
}

// ---------------------------------------------------------------------------
// pods — placement is a property of the PODS, not the workload
// ---------------------------------------------------------------------------

// Pod is where a workload actually landed.
//
// workload.spec.nodePools is a REQUEST. Billing off it bills off an intention.
// This is the fact.
type Pod struct {
	ID          string     `json:"id"`
	Name        string     `json:"name"`
	NodeName    string     `json:"nodeName"`
	Phase       string     `json:"phase"`
	StartedAt   *time.Time `json:"startedAt"`
	CompletedAt *time.Time `json:"completedAt"`

	// GPUs allocated to THIS pod. For a distributed workload this is per-worker,
	// and the workload-level GPU_ALLOCATION metric may or may not be the sum.
	// This is the number you can trust; see verify-alloc.
	RequestedGPUs *float64 `json:"requestedGpus"`
}

func (c *Client) WorkloadPods(ctx context.Context, workloadID string) ([]Pod, error) {
	var resp struct {
		Pods []Pod `json:"pods"`
	}
	q := url.Values{}
	q.Set("limit", "500")

	if err := c.get(ctx, "/api/v1/workloads/"+workloadID+"/pods", q, &resp); err != nil {
		return nil, err
	}
	return resp.Pods, nil
}
