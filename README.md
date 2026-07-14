# runai-recharge

GPU chargeback for a Run:ai cluster that spans on-premises and cloud.

Polls the Run:ai control-plane API, keeps an append-only hourly ledger of who
ran what and where, applies a rate card, and serves per-group recharge
statements plus an operator reconciliation.

Built for the case where some of your GPUs are in a rack and some are in EC2,
and the two cost different amounts.

```
recharge verify-alloc   # do this first — see below
recharge poll           # cron, every 5 min
recharge bill           # close a period
recharge serve          # /statement/{group} and /reconciliation
recharge reconcile      # had / used / billed. non-zero exit if not clean.
recharge demo           # sample statements, no DB, no cluster
```

Go, Postgres, Apache 2.0. One binary.

---

## What you get

**A PI statement** — who, what, how long, rate, amount, running total. Sectioned
by cost basis, so a lab that ran in both places sees both, separately:

```
ON-PREMISES CAPACITY                                    H100-80GB $1.05/GPU-hr
  2026-04-02  jchen    protein-fold-3     8   115,203   $1.05    $33.60   91%
  2026-04-02  mrivera  dev-workspace      1     7,200   $1.05     $2.10    3%
  On-premises capacity subtotal                163,803            $47.78

AWS BURST CAPACITY                                      H100-80GB $2.40/GPU-hr
  2026-04-05  jchen    protein-fold-4     8   115,203   $2.40    $76.80   94%
  AWS burst capacity subtotal                  385,019           $256.68

                                    April 2026 total            $304.46
                                    Fiscal year to date      $11,509.01
```

Same job shape, same silicon, same GPU-seconds — $33.60 versus $76.80. Burying
that in a blended rate means on-prem users quietly subsidize cloud burst.

The utilization column is not billed. It's there because the 3% workspace row is
the only line item in the system that changes anyone's behavior.

**A reconciliation statement** for whoever owns the budget:

```
  capacity      30,240 GPU-hours
- idle          11,164             37%   operational fact
= allocated     19,076
- unbilled         312             config gap — fix it
= billed    $22,194.30
```

**Idle and unbilled are different in kind and must never be summed.** Idle is
capacity nobody asked for; you manage it with scheduling and scale-to-zero, and
on pre-purchased hardware that money is already spent. Unbilled is allocated
GPU-hours that fell out of every statement because of a missing mapping or a
missing rate — a bug, fixable with an `INSERT`, and until it's fixed somebody
used a GPU and nobody paid for it.

`make demo` renders both from sample data with no database and no cluster.

---

## Quickstart

```sh
createdb recharge
psql recharge -f migrations/0001_schema.sql

export RECHARGE_DSN=postgres:///recharge
export RUNAI_URL=https://your-tenant.run.ai
export RUNAI_APP_ID=...
export RUNAI_APP_SECRET=...

make build
```

Define your cost bases, map your pools, set your rates:

```sql
INSERT INTO rate_class VALUES
  ('onprem',    'On-premises capacity', NULL),
  ('aws-burst', 'AWS burst capacity',   NULL);

-- AWS as a node pool inside your cluster:
INSERT INTO pool_class VALUES
  ('onprem-cluster', 'default', 'onprem',    '2026-07-01', NULL),
  ('onprem-cluster', 'aws',     'aws-burst', '2026-07-01', NULL);

-- AWS as a second Run:ai cluster? Wildcard the pool instead:
--   ('onprem-cluster', '*', 'onprem',    '2026-07-01', NULL),
--   ('aws-eks',        '*', 'aws-burst', '2026-07-01', NULL);

INSERT INTO rate (class_id, gpu_model, usd_per_gpu_hour, effective_from, note)
VALUES
  ('onprem',    'H100-80GB', 1.05, '2026-07-01', 'FY27 published rate'),
  ('aws-burst', 'H100-80GB', 2.40, '2026-07-01', 'FY27 published rate');

INSERT INTO billing_group VALUES
  ('chen-lab', 'Neuroscience / Chen Lab', 'R01-GM-114455', 'jchen@example.edu');
INSERT INTO project_group VALUES
  ('neuro-chen', 'chen-lab', '2026-07-01', NULL);
```

Then:

```sh
*/5 * * * *  recharge poll                       # the only thing that must not break
recharge reconcile -from 2026-04-01 -to 2026-05-01   # exits non-zero if not clean
recharge bill -group chen-lab -from 2026-04-01 -to 2026-05-01
recharge serve
```

Scope any read to one cost basis with `-class aws-burst` or `?class=aws-burst`.
Omit it and you get everything the group touched, sectioned.

---

## Before you trust a single number

Run:ai aggregates metrics at the workload level across worker pods. Whether
`GPU_ALLOCATION` comes back **summed across workers** or **per-pod** decides
whether a 4×8 job bills as 32 GPUs or as 8.

Submit one distributed job of a known shape, then:

```sh
recharge verify-alloc -workers 4 -gpus-per-worker 8
```

(In practice `poll` sums per-pod GPU requests, which sidesteps the ambiguity —
but verify anyway. This is exactly the kind of thing that produces a statement
off by 4× and goes unnoticed for two quarters.)

---

## How it works

```
Run:ai API ──▶ poll ──▶ usage_hour ──▶ rate ──▶ statement ──▶ web
                 │
                 └──▶ node_observation (capacity) ──▶ reconciliation
```

**GPU-seconds are computed analytically from workload start/stop timestamps**,
not by integrating the metric series. The metrics API is sampled: integrating it
adds an error of up to one sample interval to every job and leaves holes when a
scrape is missed. The workload record's timestamps are exact. The metric series
is used for exactly one thing — utilization, which is reported and never billed.

**The ledger grain is `(workload, cluster, pool, gpu_model, hour)`.** Not
workload totals. Month boundaries, mid-period rate changes, partial-month
statements, and re-running a closed statement all fall out of this for free. A
job preempted out of one pool and rescheduled into another produces rows in both
and sums correctly, with no special case.

**Placement comes from the pods, not the workload.** `spec.nodePools` is a
*request* — what the user asked for, not where the pods landed. Billing off it
bills off an intention. The poller resolves pods → node → `(cluster, pool,
gpu_model)`.

**Rates are keyed by `(rate_class, gpu_model)`.** A rate class is a set of node
pools that share a cost basis. Put every pool in one class for a single blended
institutional rate — which makes the cross-subsidy a deliberate policy choice
rather than an accident. Put AWS in its own class to recharge it separately. The
schema doesn't decide this; your `INSERT` does.

**"What they ran"** is the one thing Run:ai can't tell you. It knows the
workload; it doesn't know the work. On a shared cluster, workload names are
`test2` and `jobs-final-FINAL`. If the statement has to say what the money
bought, users must declare it at submission — Run:ai policy can require
annotations, and jobs without them don't schedule:

```yaml
required:
  annotations:
    recharge.fund-code:
    recharge.description:
```

Far easier to impose on day one than to retrofit after people have habits.
Annotations land in the ledger as JSONB, so a new required field needs no
migration. Lines without a description render as *no description declared* —
leave that visible; it's the cheapest enforcement you have.

---

## Invariants

These are load-bearing. Each has an obvious-looking simplification that quietly
loses money. If you change code near them, they're what to protect.

**Every write in the poll path is idempotent.** Crash it, double-run it, replay
a window — the ledger is unchanged. Capacity is a `COUNT` of distinct
observations, never a running sum, because a sum inflates on replay.

**The watermark advances only after a poll completes in full.** One row in
`poll_state`, written last. If poll dies mid-window, the next run re-reads the
whole window — free, precisely because of the invariant above.

**A closed period is read from `statement_line`, not re-derived.** Frozen means
frozen: a late poll or a rate correction cannot rewrite a closed month. A frozen
rate that nobody reads back is not a frozen rate.

**Nothing is ever guessed.** No default GPU model, no inferred placement, no
assumed pool. A pod on an unknown node becomes an `orphan_pod` and does not
bill. Ledger rows that join no rate are `Unbilled` and do not bill. Both appear
on the reconciliation. *In a billing ledger, a missing value must never have a
default* — an error you have to fix is cheap; a plausible number you never
question is not.

**Authz fails closed.** Every route 404s until `Server.Authz` is wired to your
SSO. For a single-operator pilot, `RECHARGE_INSECURE_ADMIN=yes-i-mean-it` — which
logs a warning on every start, and which you have to mean.

---

## Status

Working, and unverified against a live cluster.

The ledger arithmetic is tested (mass conservation across multi-week jobs,
fractional GPU, pod-order independence, replay safety). The Run:ai client is
not: field names and pagination semantics vary by cluster version, and
`internal/runai` is where to look first if numbers come out wrong. Start with
`verify-alloc`.

`Server.Authz` is a stub that denies. Wire it before anyone but you can reach the
port.

## License

Apache 2.0. Copyright 2026 Scott Friedman. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
