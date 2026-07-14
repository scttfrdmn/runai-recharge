-- runai-recharge: schema
--
-- Five tables you write, four the poller owns, three views.
--
-- The invariants this schema exists to protect:
--
--   1. usage_hour is the grain: (workload, cluster, pool, gpu_model, hour).
--      Not workload totals. Month boundaries, mid-period rate changes,
--      partial-month statements, and re-running a closed statement all fall out
--      of this grain for free. Workload-total rows would force splitting logic
--      you will get subtly wrong on the job that ran for six weeks.
--
--   2. Every write in the poll path is idempotent. Crash it, double-run it,
--      replay a window -- the ledger is unchanged. Capacity is a COUNT of
--      distinct observations, never a running sum, because a sum inflates on
--      replay.
--
--   3. Rates are temporal; statements freeze the rate as a VALUE. Somebody will
--      audit FY26 in FY29.
--
--   4. Run:ai `project` is NOT the billing group. Project is a scheduling and
--      quota construct. The billing group is a PI, a lab, a fund code. They
--      coincide today and won't forever -- one project, three grants.
--
--   5. Nothing is ever guessed. There are no DEFAULTs on anything that affects
--      money. In a billing ledger a missing value must never have a default: an
--      error you have to fix is cheap, a plausible number you never question is
--      not.

BEGIN;

-- ===========================================================================
-- CONFIGURATION -- you write these
-- ===========================================================================

-- A rate class is a set of node pools that SHARE A COST BASIS -- i.e. that get
-- charged at the same rate.
--
-- On-prem H100s and AWS H100s are the same silicon and (probably) a different
-- number. Put every pool in one class and you get a single blended
-- institutional rate, with the cross-subsidy a deliberate policy choice rather
-- than an accident. Put AWS in its own class and it's recharged separately.
--
-- This schema does not make that decision. The INSERT does.
CREATE TABLE rate_class (
    class_id  TEXT PRIMARY KEY,          -- 'onprem', 'aws-burst', or 'default'
    name      TEXT NOT NULL,             -- shown on statements
    note      TEXT
);

-- (cluster, pool) -> rate class. Temporal: pools get reorganized, and a
-- statement re-run in FY29 must resolve as of the hour it bills.
--
-- Handles both Run:ai topologies:
--   AWS as a node pool in your cluster -> match on node_pool
--   AWS as a second Run:ai cluster     -> match on cluster_id, node_pool = '*'
CREATE TABLE pool_class (
    cluster_id     TEXT        NOT NULL,
    node_pool      TEXT        NOT NULL,   -- '*' matches any pool in the cluster
    class_id       TEXT        NOT NULL REFERENCES rate_class(class_id),
    effective_from TIMESTAMPTZ NOT NULL,
    effective_to   TIMESTAMPTZ,
    PRIMARY KEY (cluster_id, node_pool, effective_from)
);

-- A rate is just a rate. This tool does not derive it; you set it.
CREATE TABLE rate (
    class_id         TEXT        NOT NULL REFERENCES rate_class(class_id),
    gpu_model        TEXT        NOT NULL,
    usd_per_gpu_hour NUMERIC(10,4) NOT NULL,
    effective_from   TIMESTAMPTZ NOT NULL,
    effective_to     TIMESTAMPTZ,          -- NULL = open-ended
    note             TEXT,

    PRIMARY KEY (class_id, gpu_model, effective_from),
    CHECK (effective_to IS NULL OR effective_to > effective_from)
);

-- At most one open rate per (class, model).
CREATE UNIQUE INDEX rate_no_overlap_idx ON rate (class_id, gpu_model)
    WHERE effective_to IS NULL;

CREATE TABLE billing_group (
    group_id   TEXT PRIMARY KEY,          -- 'chen-lab'
    name       TEXT NOT NULL,             -- 'Neuroscience / Chen Lab'
    fund_code  TEXT,
    pi_email   TEXT
);

CREATE TABLE project_group (
    project        TEXT        NOT NULL,  -- Run:ai project
    group_id       TEXT        NOT NULL REFERENCES billing_group(group_id),
    effective_from TIMESTAMPTZ NOT NULL,
    effective_to   TIMESTAMPTZ,
    PRIMARY KEY (project, effective_from)
);

-- ===========================================================================
-- LEDGER -- the poller owns these
-- ===========================================================================

-- Where the poll got to. ONE row, advanced only after a poll completes in full.
--
-- If poll dies mid-window the next run re-reads the whole window, which is free
-- because every write is idempotent. Deriving this from max(last_polled_at)
-- instead would mean a poll that ingested 1 of 500 and crashed advanced past
-- the 499 it never wrote -- and they would fall outside every subsequent window
-- forever.
CREATE TABLE poll_state (
    id           INTEGER PRIMARY KEY DEFAULT 1,
    watermark    TIMESTAMPTZ NOT NULL,
    last_success TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (id = 1)
);

-- Node inventory. The only thing that knows where a GPU is and what model it is.
-- Never deleted: in AWS nodes scale to zero and return with new names, and a
-- node that vanished must still resolve for the hours it served.
CREATE TABLE node (
    node_name  TEXT PRIMARY KEY,
    cluster_id TEXT NOT NULL,
    node_pool  TEXT NOT NULL,
    gpu_model  TEXT NOT NULL,             -- nvidia.com/gpu.product
    gpu_count  INTEGER NOT NULL,
    seen_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Capacity: what we HAD. One row per (node, poll slot).
--
-- Idempotent by construction. Capacity is COUNT(observations) * slot_seconds,
-- so replaying a poll is a no-op. An accumulating fraction would inflate.
--
-- CAVEAT, and it is real: capacity is sampled at the poll interval, so a node
-- that lived and died between two polls is invisible here. With Karpenter churn
-- on spot that is not hypothetical. Allocation, by contrast, is exact.
CREATE TABLE node_observation (
    node_name    TEXT        NOT NULL,
    slot_start   TIMESTAMPTZ NOT NULL,
    slot_seconds INTEGER     NOT NULL,

    cluster_id   TEXT        NOT NULL,
    node_pool    TEXT        NOT NULL,
    gpu_model    TEXT        NOT NULL,
    gpu_count    INTEGER     NOT NULL,

    PRIMARY KEY (node_name, slot_start)
);

CREATE INDEX node_obs_scope_idx ON node_observation (cluster_id, node_pool, slot_start);

-- Workload identity: WHO and WHAT.
CREATE TABLE workload (
    workload_id    TEXT PRIMARY KEY,

    submitter      TEXT NOT NULL,         -- SSO subject
    project        TEXT NOT NULL,         -- Run:ai project (scheduling, not billing)
    department     TEXT,

    cluster_id     TEXT NOT NULL,
    node_pool      TEXT,                  -- REQUESTED. Not billed off. See usage_hour.

    name           TEXT NOT NULL,         -- user-chosen; usually 'test2'
    workload_type  TEXT,                  -- Training | Workspace | Inference
    image          TEXT,                  -- more honest than name

    -- Enforced at submission via Run:ai policy. This is where
    -- recharge.fund-code and recharge.description live -- the only way to get a
    -- defensible answer to "what did the money buy." JSONB so a new required
    -- field needs no migration.
    annotations    JSONB NOT NULL DEFAULT '{}'::jsonb,

    started_at     TIMESTAMPTZ NOT NULL,
    completed_at   TIMESTAMPTZ,           -- NULL while running
    phase          TEXT NOT NULL,

    first_seen_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_polled_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX workload_open_idx ON workload (completed_at) WHERE completed_at IS NULL;
CREATE INDEX workload_proj_idx ON workload (project, started_at);
CREATE INDEX workload_user_idx ON workload (submitter, started_at);

-- THE LEDGER. Hourly grain, placement included.
--
-- cluster_id / node_pool / gpu_model are resolved AT INGEST from the node the
-- pods actually landed on -- never from workload.node_pool, which is a REQUEST.
-- Billing off a request is billing off an intention.
--
-- Because placement is part of the key, a job preempted out of one pool and
-- rescheduled into another produces rows in BOTH and sums correctly, with no
-- special case anywhere.
CREATE TABLE usage_hour (
    workload_id   TEXT        NOT NULL REFERENCES workload(workload_id),
    cluster_id    TEXT        NOT NULL,
    node_pool     TEXT        NOT NULL,
    gpu_model     TEXT        NOT NULL,
    hour_start    TIMESTAMPTZ NOT NULL,   -- UTC, truncated

    gpu_alloc     NUMERIC(10,4) NOT NULL, -- fractional-GPU safe
    seconds       NUMERIC(12,3) NOT NULL, -- wall seconds in this hour
    gpu_seconds   NUMERIC(14,3) NOT NULL, -- seconds * gpu_alloc   [BILLABLE]

    gpu_util_mean NUMERIC(5,2),           -- 0-100. REPORTED, NEVER BILLED.

    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (workload_id, cluster_id, node_pool, gpu_model, hour_start),
    CHECK (seconds >= 0 AND seconds <= 3600.001),
    CHECK (gpu_seconds >= 0)
);

CREATE INDEX usage_hour_time_idx  ON usage_hour (hour_start);
CREATE INDEX usage_hour_scope_idx ON usage_hour (cluster_id, node_pool, hour_start);

-- Pods we could not place. Do not downgrade this to a log line: a billing gap
-- that lives only in stderr is a billing gap nobody will ever find. It goes on
-- the reconciliation statement, where an operator has to look at it.
CREATE TABLE orphan_pod (
    workload_id   TEXT        NOT NULL,
    pod_name      TEXT        NOT NULL,
    node_name     TEXT,
    workload_name TEXT,
    submitter     TEXT,
    project       TEXT,
    started_at    TIMESTAMPTZ,
    first_seen    TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen     TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved      BOOLEAN     NOT NULL DEFAULT false,

    PRIMARY KEY (workload_id, pod_name)
);

CREATE INDEX orphan_open_idx ON orphan_pod (last_seen) WHERE NOT resolved;

-- ===========================================================================
-- STATEMENTS -- frozen at close. Never updated.
-- ===========================================================================

CREATE TABLE statement (
    statement_id BIGSERIAL PRIMARY KEY,
    period_start TIMESTAMPTZ NOT NULL,
    period_end   TIMESTAMPTZ NOT NULL,
    group_id     TEXT        NOT NULL REFERENCES billing_group(group_id),
    class_id     TEXT        REFERENCES rate_class(class_id),  -- NULL = all capacity
    closed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (period_start, period_end, group_id, class_id)
);

CREATE INDEX statement_lookup_idx ON statement (group_id, period_start, period_end);

-- rate_applied is a VALUE, not a foreign key to a rate row that may since have
-- changed. Once a period is closed, the read path serves THESE rows -- a late
-- poll or a rate correction cannot rewrite a closed month.
CREATE TABLE statement_line (
    statement_id  BIGINT      NOT NULL REFERENCES statement(statement_id) ON DELETE CASCADE,
    line_no       INTEGER     NOT NULL,

    workload_id   TEXT        NOT NULL,
    submitter     TEXT        NOT NULL,
    workload_name TEXT        NOT NULL,
    workload_type TEXT,
    description   TEXT,                   -- from annotations
    fund_code     TEXT,                   -- from annotations
    gpu_model     TEXT        NOT NULL,
    node_pool     TEXT,
    class_id      TEXT,
    gpu_alloc     NUMERIC(10,4) NOT NULL,

    started_at    TIMESTAMPTZ NOT NULL,
    gpu_seconds   NUMERIC(14,3) NOT NULL,
    gpu_util_mean NUMERIC(5,2),

    rate_applied  NUMERIC(10,4) NOT NULL, -- FROZEN
    amount_usd    NUMERIC(14,4) NOT NULL,

    PRIMARY KEY (statement_id, line_no)
);

CREATE INDEX statement_line_workload_idx ON statement_line (workload_id);

-- ===========================================================================
-- VIEWS
-- ===========================================================================

-- Attach a rate class to every ledger row. Exact pool mapping beats the
-- wildcard. Every query uses this; do not inline the precedence logic.
--
-- class_id IS NULL means the pool has no mapping -- which means those GPU-hours
-- join no rate and appear on no statement. That is not silent: it is what
-- `recharge reconcile` reports.
CREATE VIEW usage_classified AS
SELECT
    u.*,
    COALESCE(pc_exact.class_id, pc_wild.class_id) AS class_id
FROM usage_hour u
LEFT JOIN pool_class pc_exact
       ON pc_exact.cluster_id = u.cluster_id
      AND pc_exact.node_pool  = u.node_pool
      AND u.hour_start >= pc_exact.effective_from
      AND (pc_exact.effective_to IS NULL OR u.hour_start < pc_exact.effective_to)
LEFT JOIN pool_class pc_wild
       ON pc_wild.cluster_id = u.cluster_id
      AND pc_wild.node_pool  = '*'
      AND u.hour_start >= pc_wild.effective_from
      AND (pc_wild.effective_to IS NULL OR u.hour_start < pc_wild.effective_to);

-- Capacity per hour, derived from a COUNT of observations.
CREATE VIEW node_capacity AS
SELECT
    date_trunc('hour', slot_start) AS hour_start,
    cluster_id,
    node_pool,
    gpu_model,
    sum(gpu_count * slot_seconds) / 3600.0 AS gpu_hours
FROM node_observation
GROUP BY 1, 2, 3, 4;

COMMIT;
