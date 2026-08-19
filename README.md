# Mercado Luna — Smart Load Distribution Service

A payment-orchestration layer that sits between Mercado Luna's checkout and its
payment processors. It distributes authorization traffic across multiple PSPs,
never exceeds any processor's rate limit, and fails over automatically the
moment a processor starts degrading — no human in the loop.

Built for the "Midnight Spike" challenge: 40,000 checkout attempts in the first
five minutes of a flash sale, against a primary processor that caps at 150 TPS.

---

## Quick start

```bash
pip install -r requirements.txt

./run.sh                    # terminal 1: 3 mock PSPs + the router
python3 -m loadgen.demo     # terminal 2: the four-scenario demo
```

First boot takes ~30s (four separate uvicorn processes). `run.sh` reports each
service as it comes up and exits with an error if any fails to start.

The demo prints acceptance checks as it goes and exits non-zero if any fail.

Other entry points:

```bash
python3 -m loadgen.generate --profile spike        # the flash-sale ramp
python3 -m loadgen.generate --rps 450 --seconds 10 # constant rate
python3 -m loadgen.generate --concurrent 500       # 500 unpaced simultaneous
python3 -m loadgen.demo --only failure             # one scenario

curl localhost:8080/metrics/summary                # human-readable state
curl localhost:8080/metrics | jq                   # full JSON state
```

---

## Architecture

```
                Mercado Luna checkout
                          |
                          v
        +-----------------------------------------+
        |   Smart Load Distribution Service       |
        |                                         |
        |   admission control  (shed early)       |
        |   selection          (health -> cost)   |
        |   sliding-window limiter  (per PSP)     |
        |   circuit breaker         (per PSP)     |
        |   failover               (1 retry hop)  |
        |   synthetic recovery probes             |
        +-----------------------------------------+
             |               |               |
             v               v               v
        Atlas Pay      Borealis Pay    Cygnus Financial
        150 rps         100 rps          200 rps
        2.5% fee        2.9% fee         2.1% fee
```

The whole router is a **single asyncio process** holding all state in memory.
That is a deliberate choice, discussed under Tradeoffs.

### The request path

1. **Admission control.** If the lane's queue is already full, reject
   immediately with `503 system_at_capacity`. Shedding early is the point —
   see "Why the queue is shallow" below.
2. **Selection.** Filter out processors whose circuit breaker is not `CLOSED`,
   sort the survivors by preference, then walk the sorted list reserving a
   capacity slot until one succeeds.
3. **Queue.** If nothing has capacity, park until a slot frees or the deadline
   (2.5s normal / 5s priority) expires, then shed.
4. **Dispatch** to the chosen processor.
5. **Record** the outcome into that processor's rolling health window, which may
   trip its circuit breaker.
6. **Failover** on a retryable failure: one more hop to a different processor,
   carrying the same idempotency key.

### Files

| Path | Role |
|---|---|
| `router/ratelimit.py` | Sliding-window limiter + in-flight bound — Requirement 1 |
| `router/health.py` | Rolling health windows + circuit breaker — Requirement 2 |
| `router/pool.py` | Processor pool and the selection rule |
| `router/service.py` | Admission control, dispatch, failover, recovery probes |
| `router/upstream.py` | PSP client and outcome classification (the retry policy) |
| `router/main.py` | HTTP surface: `/v1/payments/authorize`, `/metrics`, `/healthz` |
| `mock_psp/main.py` | Mock PSP: enforces its own limit, models declines, `/admin/degrade` |
| `loadgen/generate.py` | Multi-process traffic generator + rate-limit verification |
| `loadgen/demo.py` | The four-scenario narrated demo |
| `config/processors.json` | All tunables in one place |

---

## Verified results

`python3 -m loadgen.demo` — **18/18 acceptance checks pass**. All figures below
are from one actual run.

### Rate limits: 12 verdicts, zero rejections

Reported by the processors, whose 429 counters are the only honest witness:

| Scenario | Atlas (150) | Borealis (100) | Cygnus (200) | Rejections |
|---|---|---|---|---|
| Normal, 120 rps | 22 | 15 | 110 | **0** |
| Midnight spike, 500 rps | 113 | 69 | 193 | **0** |
| Processor failure mid-sale | 143 | 88 | 181 | **0** |
| After recovery | 81 | 57 | 160 | **0** |

The failure row is the hard case: when a circuit opens, the whole load shifts
onto the survivors in one step. That surge defeated three earlier limiter
designs before the invariant made it structural.

### Latency

| Scenario | p50 | p95 | p99 |
|---|---|---|---|
| Normal, 120 rps | 21ms | 36ms | 67ms |
| Midnight spike, 500 rps | 836ms | 5000ms | 5091ms |
| Processor failure mid-sale | 28ms | 144ms | 342ms |
| After recovery | 194ms | 2633ms | 5021ms |

The failure scenario at p50 28ms is the number I care about most: the busiest
processor died and the median checkout was unaffected.

### Failure detected and absorbed without a human

Live monitor, failure induced at t=5.0 (`in/s` = offered rate, per-processor
cells are `current/limit`):

```
        t   in/s      ok   decl   shed   fail     q        atlas    borealis      cygnus
      4.0    200     679     67      0      1     0   29/127      20/85      155/170
  --- inducing failure on psp-cygnus ---
      6.0    200    1048     96      0      3     0  125/127      53/85      162/170
      8.0    199    1411    128      0     10     0  127/127      44/85      166/170
```

Atlas goes from 29 to 125 rps within one second, saturating at its ceiling.
Mercado Luna's Week 3 failover took 12 minutes and cost $180K.

### Automatic recovery

```
breakers before repair: atlas=closed  borealis=closed  cygnus=open
>> psp-cygnus has been repaired
waiting for the router to notice on its own (probe cooldown is 30s)...
psp-cygnus breaker is now: closed
```

The router was never told the processor was fixed. Synthetic probes found out.

### Cost (Stretch A)

```
normal lane    paid 30,042.84 vs 35,534.11 all-baseline -> saved 5,491.27 MXN (15.45%)  blended 2.1137%
priority lane  paid 88,921.96 vs 83,533.96 all-baseline -> premium 5,388.00 MXN (routed for reliability)
```

Reported per lane after a single blended figure proved misleading: it read
"saved −1,581 MXN", which looked like cost optimisation losing money. In fact
normal traffic saves 15.45% while the priority lane knowingly pays a premium —
priority baskets are the high-value ones and route to the most reliable
processor regardless of fee. Averaging the lanes hid both facts.

### One target not met: the queue deadline is soft

Stated plainly because the numbers are in the output and I would rather name it
than have a reviewer find it:

| Lane | Budget | Max observed | Over budget |
|---|---|---|---|
| normal | 2500ms | 3005ms | 86.6% of queued requests |
| priority | 5000ms | 5497ms | 71.8% of queued requests |

The overshoot is ~500ms, about 20% of budget, and it applies only to requests
that queued at all (2,103 of 7,370 during the spike). It is **not** a logic
error — the deadline is re-checked after every wake and the sleep is clamped to
the remaining time. It is event-loop lag: a `sleep(10ms)` does not resume in
10ms when the loop is saturated, so the wait lands late no matter what the
comparison says.

Two rounds of work took it from 816ms of overshoot to ~500ms (clamping the
sleep, and cutting the queue from 450 to 150 so fewer coroutines poll). The
remaining gap needs the polling loop replaced with event-driven handoff, which
is a concurrency change I was not willing to make against a verified build. It
is the first item under future work.

The brief asks for "queued briefly (e.g. 2-3 seconds max)". At 3005ms the normal
lane is marginally outside that. Worth knowing rather than glossing.

---

## Design decisions

### A decline is not a failure

This is the most consequential decision in the service.

The brief says to track "success rate" and pull a processor below a threshold.
Read literally that is wrong for payments. A card declined for insufficient
funds, a fraud block, or an expired card is a **correct** answer from a
perfectly healthy processor — the issuing bank said no.

A midnight flash sale produces a wave of exactly those declines, as shoppers
hammer maxed-out cards. If declines fed the circuit breaker, the healthiest
processor would be removed from rotation at the worst possible moment and the
service would manufacture the outage it exists to prevent.

So two signals are tracked separately:

| Signal | Definition | Effect |
|---|---|---|
| `technical_success_rate` | `(approved + declined) / total` | **Drives the breaker.** Timeouts, 5xx, connection errors and 429s are the failures. |
| `auth_rate` | `approved / (approved + declined)` | Reported. A collapsed auth rate *deprioritises* a processor but never opens the breaker — the cause is usually an issuer or a BIN range, and cutting the PSP off would not help. |

Both are exposed per processor on `/metrics`.

### Sliding window, not a token bucket

A token bucket with burst `B` and rate `R` can emit `B` tokens instantly and
then `R`/second, so within one badly-aligned second an upstream can observe up
to `B + R` arrivals. The PSPs measure their own limit over a trailing one-second
window, so a token bucket trips their 429 path even when the configured rate
looks correct.

A sliding window log measures exactly what the upstream measures. On top of
that, headroom is held back to absorb in-flight drift: a request counted at `t`
may not land at the PSP until `t + latency`, so the PSP's one-second window can
contain dispatches from two different windows of ours.

**The headroom figure is measured, not guessed — and 5% was not enough.** During
the failure scenario, at the instant Cygnus's circuit opened and the entire load
shifted onto Atlas, Atlas was dispatched 142/s by the router and observed **156**
arrivals in its own window: 14 requests of drift, and 24 rate-limit rejections.
The worst case for this service is not steady state, it is the moment traffic
moves. Headroom is therefore **15%** (150 → 127, 100 → 85, 200 → 170; in-flight caps 31 / 21 / 42), which
covers the measured drift with room to spare at the cost of leaving some
advertised capacity unused. Adaptive headroom would recover it; see future work.

**And raising headroom was the wrong fix.** At 15% Atlas got *worse* — 168
arrivals, 143 rejections — because the drift scales with how far behind the
router's event loop is, not with the configured rate. Under saturation a
coroutine reserves a slot at `t` and does not put bytes on the wire until `t` +
several hundred ms, so slots reserved across many router-seconds land inside one
PSP-second. No sender-side rate window can fix that, because it measures at the
wrong moment.

The fix is to bound **concurrency** as well as rate: at most N requests to a
processor may be outstanding, so request N+1 physically cannot be sent until one
of the first N returns. No scheduling delay can conjure a burst bigger than N.

| Configuration | Atlas peak (limit 150) | Rejections |
|---|---|---|
| 5% headroom, no in-flight bound | 156 | 24 |
| 15% headroom, no in-flight bound | 168 | 143 |
| 10% headroom + in-flight bound | 131 | 0 |
| **15% headroom + in-flight bound (final)** | **within limit** | **0** |

The in-flight bound also fixed latency and load shedding as a side effect: p50
fell from 1196ms to 428ms and the service began shedding excess traffic instead
of accepting work it could not dispatch. Bounding concurrency is what turns
collapse into backpressure.

This is the whole value of making the processors the witness. The router believed
it was correct through two wrong fixes; the processor proved it was not, both
times.

The result is the claim below, and it holds because the processors themselves
report it, not because the router asserts it.

### The processors are the witness

Each mock PSP enforces its own rate limit and returns `429` with a
`rate_limit_rejections` counter. Every load test ends with:

```
  RATE LIMIT VERIFICATION (reported by the processors themselves)
    processor         limit  peak rps   429s  received  verdict
    psp-atlas           150        64      0       604  PASS
    psp-borealis        100        29      0       403  PASS
    psp-cygnus          200       168      0      3545  PASS

    VERDICT: PASS -- no processor was ever sent more than its configured limit
```

A router claiming it respects rate limits is worth nothing. The processor
saying it never had to reject one is worth something. The router also counts
`upstream_rate_limit_breaches` itself and logs any 429 at `ERROR`, because a
429 means its own limiter is mis-sized.

### Why the queue is shallow

The queue holds ~450 requests, a little over one second of capacity, not thousands.

A 2000-deep queue in front of ~382 rps guarantees ~4.7 second waits under
overload: every shopper gets a spinner and nobody gets an answer. Sized to the
latency budget instead, the excess is rejected in milliseconds with an honest
"system at capacity". For checkout, a fast failure the client can retry beats a
slow success nobody waited for.

There is also no separate queue object and worker pool. Each in-flight request
parks itself in `_acquire_processor` until capacity frees up; the count of
parked requests **is** the queue depth. Fewer moving parts, and no way for a
queue and a worker pool to disagree about who owns a request.

`queued` and `inflight` are tracked as distinct numbers. Conflating them was a
real bug during development: shedding triggered on total concurrency instead of
on the backlog, so the queue never appeared full and nothing was ever shed.

The deadline is also **request-scoped, not attempt-scoped**. Giving each retry
hop a fresh 2.5s meant a retried request could legitimately wait twice the
budget — observed max queue wait was 5194ms against a stated 2500ms. A latency
budget that silently doubles on retry is not a budget.

### Retry policy

Failover is one extra hop, and only for outcomes where the bank's answer is
unknown:

| Outcome | Retryable | Why |
|---|---|---|
| Approved | no | Terminal. |
| **Declined** | **no** | The bank answered. Retrying the same card elsewhere is almost always declined again, adds load during an incident, and can look like card testing to a fraud system. |
| Timeout / connection error / 5xx / 429 | yes | The processor never returned the bank's answer. Safe to try elsewhere. |
| 4xx (malformed) | no | Our bug. Retrying will not fix it. |

The **same idempotency key** travels across the failover hop, and the mock PSPs
honour it — a replay returns the stored authorization and increments
`idempotent_replays` instead of authorising twice. Unlike a web load balancer, a
retry here moves real money.

### Synthetic recovery probes

A processor with an open circuit is probed on a 30s cooldown with a small
**synthetic** authorization, not with a real shopper's payment. Borrowing live
traffic to test a processor known to be failing spends a genuine checkout to
gather telemetry. Two healthy probes close the circuit; a failed probe resets
the cooldown.

### Cost-aware routing (Stretch A)

Default strategy `cost_aware` orders healthy processors by fee, so traffic fills
the cheapest (Cygnus, 2.1%) before spilling to Atlas and then Borealis. Under
flash-sale load all three saturate anyway, so distribution ends up proportional
to capacity — cost optimisation costs nothing when it matters and helps when
volume is low.

`/metrics` reports fees paid versus a baseline of routing everything to the
primary processor, plus the blended effective fee.

`routing_strategy: "balanced"` in the config flips the ordering to
least-utilised-first for anyone who would rather keep all three processors warm.

### Priority lanes (Stretch B)

Requests above 5,000 MXN are flagged `priority: true` by the generator and get
a separate admission allowance, a longer deadline (5s vs 2.5s), and are routed
by reliability and headroom rather than by fee. This is preferential admission,
not strict FIFO ordering — stated plainly rather than overclaimed.

### Sampled logging

At 500 rps, logging every routing decision makes the console unreadable and the
log writer itself becomes a bottleneck: the observability damages the thing
being observed. Routine `ROUTE` lines are sampled 1-in-25. Everything that
matters is logged unconditionally — circuit state changes, failovers, load
shedding, probes, and rate-limit breaches. Priority and multi-attempt requests
are never sampled out.

---

## The load generator is the bottleneck, not the router

Worth stating plainly, because it shaped the harness.

Measured on the dev machine: **one Python process using httpx tops out near 150
rps** of offered load. The ceiling is per-request client overhead. The mock
PSPs, once on uvloop + httptools, absorbed **561 rps** in the same test.

So a single-process generator cannot produce a 500 rps flash sale, and any
"we hit 500 rps" claim from one would be false. The generator shards its target
rate across `--workers` OS processes (default 6) and aggregates the results.

Two related environment findings:

- **`kern.ipc.somaxconn` is 128 on macOS.** An unbounded connection pool opens a
  fresh socket per request, overruns the listen backlog, and the generator
  reports `ConnectError` — which reads as the service refusing traffic when it is
  really the loopback socket layer giving up. Fixed with a bounded keep-alive
  pool per worker, which is what a real checkout service would use anyway.
- **uvloop and httptools are not optional here.** On the stock asyncio loop with
  the pure-Python h11 parser, a mock PSP served 84 rps — below its own advertised
  150. `run.sh` starts every service with `--loop uvloop --http httptools`.

---

## Tradeoffs

**Single process, in-memory state.** All counters, windows and breakers live in
one asyncio process. Rate limiting is only correct if one process owns the
count, so this is the honest single-node design: no locks, no races, no
distributed-counter drift. Scaling out needs shared state (Redis token buckets,
or a capacity share per replica) — that is the first thing I would build next
and it is a genuinely different design, not a config change.

**Polling queue, and it does not scale.** Waiting requests poll every 10ms
rather than waiting on a condition variable. I originally wrote that this "costs
up to 10ms of latency". Measured, it cost **~800ms**: at a 450-deep queue, 450
coroutines waking every 10ms is ~45,000 wakeups/second, and that load is itself
most of the event-loop lag. The queue was generating the latency that broke its
own deadline -- 89.4% of queued requests exceeded the 2500ms budget, up to 3316ms.

Two changes rather than one. The deadline is now re-checked after waking and the
sleep is clamped to the remaining time, so the logic cannot overshoot. And the
queue was cut to 150 (about 0.4s of capacity), which caps the wakeup load and
sheds earlier. Both were needed: the second is what actually removes the lag.

The proper fix is event-driven handoff -- signal waiters when a processor
releases capacity, so a waiting request costs one wakeup instead of one per
10ms. That is the next change I would make, and it is the honest reason the
queue is shallow rather than a design preference.

**Health is per-processor, not per-BIN.** In LATAM the biggest lever on
authorization rates is routing by card BIN and issuer to a local acquirer. That
is real payment orchestration and it is out of scope here; the hooks exist
(`card_bin` is carried end to end) but nothing uses it yet.

**15% headroom is a static, measured constant.** It was raised from 5% after the
failover surge above proved 5% insufficient. It should adapt to observed latency
rather than being a fixed factor sized for the worst case, which leaves capacity
unused in steady state.

**The mock PSPs are not real PSPs.** No 3DS, no capture/settlement split, no
network tokens, no soft-decline taxonomy worth the name.

---

## What I would do next, in order

1. **Event-driven queue handoff**, replacing the polling loop. Signal waiters
   when a processor releases capacity so a queued request costs one wakeup
   instead of one every 10ms. This is the only measured target the service does
   not meet (see "One target not met" above) and it is a known, bounded change.
2. **Shared-state rate limiting** so the router can run more than one replica.
   This is the only thing standing between this and production.
3. **BIN-level routing.** Track auth rate per (processor × BIN range × country)
   and route on it. In Mexico this is worth more than everything else here
   combined.
4. **Adaptive selection.** The selection rule is a fixed sort. A contextual
   bandit over processors — Thompson sampling on approval as the reward —
   explores and exploits automatically and would beat static ordering, without
   putting a model on the hot path.
5. **Retry budgets.** Failover is capped per request but not globally. During a
   wide incident, retries add load exactly when there is none to spare.
6. **Reconciliation.** Idempotency keys are honoured, but a timeout leaves a
   genuinely unknown state that only a settlement-file reconciliation resolves.
7. **Adaptive headroom and in-flight bound** driven by measured latency,
   recovering the ~15% of advertised capacity the static invariant reserves.

---

## Requirements coverage

| Requirement | Where | Demonstrated by |
|---|---|---|
| Route across ≥3 processors | `pool.py` | every scenario; distribution table |
| Respect per-processor rate limits | `ratelimit.py` | PSP-reported `429s = 0` |
| Queue briefly when saturated | `service.py` | `avg_queue_wait_ms` on `/metrics` |
| Shed with a clear response when queues fill | `service.py` | `503 system_at_capacity`, scenario 2 |
| Rolling success-rate tracking | `health.py` | `/metrics` per-processor health |
| Stop routing to failing processors | `health.py` | scenario 3 |
| Redistribute to healthy processors | `pool.py` | scenario 3 distribution |
| Periodic retry of unhealthy processors | `service.py` probe loop | scenario 4 |
| Resume on recovery | `health.py` | scenario 4 |
| Stretch A — cost-optimised routing | `pool.cost_report()` | `cost` block on `/metrics` |
| Stretch B — priority lanes | `service.py` | `priority: true` requests |
| Stretch C — real-time metrics API | `main.py` | `/metrics`, `/metrics/summary` |
