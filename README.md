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
Committed copies are in the repo root — but GitHub shows `.html` as source, so
to see them **rendered**, open them through a proxy:

- [Statement](https://htmlpreview.github.io/?https://github.com/scttfrdmn/runai-recharge/blob/main/statement-sample.html)
- [Reconciliation](https://htmlpreview.github.io/?https://github.com/scttfrdmn/runai-recharge/blob/main/reconciliation-sample.html)

---

## Quickstart

**Prerequisites**

- **Go 1.25+** — to `make build` the binary (see `go.mod`).
- **PostgreSQL 16+** — the ledger. Any reachable instance; `createdb`/`psql`
  below assume the client tools and a local server. CI runs against 16.
- **Run:ai application credentials** — an app with read access to workloads,
  pods, nodes, and metrics on your tenant. `RUNAI_APP_ID` / `RUNAI_APP_SECRET`
  come from the Run:ai UI; `RUNAI_URL` is your control-plane URL. Only `poll`
  and `verify-alloc` talk to Run:ai — `bill`, `reconcile`, `serve`, and `demo`
  need Postgres alone, and `demo` needs neither.

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

**Every guess is instrumented.** This system guesses constantly — where a pod
ran, which rate applies, whether the API paginates the way we think. It cannot
avoid that; it runs against an API whose behavior varies by cluster version. What
it refuses to do is guess *silently*. Every place we don't know, and can't know
yet, is wired so the not-knowing becomes visible the moment it starts costing
money — as an error you have to clear, never as a zero you'll never question.
`orphan_pod`, `Unbilled`, `verify-alloc`, and the pagination tripwire are all the
same move. That is the invariant the rest of these protect.

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

**A missing value never gets a default.** No default GPU model, no inferred
placement, no assumed pool. A pod on an unknown node becomes an `orphan_pod` and
does not bill. Ledger rows that join no rate are `Unbilled` and do not bill. Both
appear on the reconciliation. *In a billing ledger, a missing value must never
have a default* — an error you have to fix is cheap; a plausible number you never
question is not.

**Pagination fails loudly.** The Run:ai workload list is paged, and the scheme
varies by cluster version — we can't know it without the cluster in front of us.
So `ListWorkloads` doesn't try to be clever: it stops only on a short page, and a
*full* page that produced no new workloads is a hard error, not a silent break. A
wrong scheme truncates a month to its first page and looks completely plausible;
this turns that into a failed poll on day one. The dedup-by-ID that measures
"new workloads" is load-bearing, not a redundant guard — remove it and the
tripwire goes blind.

**Authz fails closed.** Every route 404s until `Server.Authz` is wired to your
SSO. For a single-operator pilot, `RECHARGE_INSECURE_ADMIN=yes-i-mean-it`
disables authz — and because that's world-readable financial data, it is
*pilot-only, which means loopback-only*: with the hatch open, `serve` **refuses
to start** on a non-loopback `RECHARGE_ADDR` unless you also set
`RECHARGE_INSECURE_ADMIN_BIND_ANYWHERE=yes-i-mean-it`. Put a real proxy in front
instead. While the hatch is open the warning fires on *every request*, not once
at boot where it scrolls away.

**Every instrument is read.** Detecting a gap is half the job; the other half is
someone hearing about it. All three tripwires are exposed for whatever you
already run for monitoring, and `/healthz`/`/metrics` are the only routes not
behind authz — a scraper carries no SSO identity, and they expose counts, not
billing data:

- `GET /healthz` **503s when the poll has stalled** (last success older than 3×
  the interval). The poll is the only unrecoverable component — Run:ai ages its
  source data out — and a poll that fails for *any* reason, tripwire included,
  simply stops advancing the watermark, so staleness catches every failure mode
  at once. Wire it to your uptime check; a 503 is a page-someone event.
- `GET /metrics` emits `recharge_poll_last_success_timestamp`,
  `recharge_poll_stale`, `recharge_orphan_pods`, `recharge_unbilled_gpu_hours`,
  and `recharge_unbilled_gaps`. Alert on the last two before close-of-month, not
  at it.

The same rule applies to the tests: the integration test skips with no
`RECHARGE_TEST_DSN` on a laptop, but **fails rather than skips under CI** — a
green build that silently ran nothing is the same silent zero everything here
exists to prevent.

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
