"""Flash-sale traffic generator.

Paces requests to a target rate rather than firing an unbounded burst, because
"500 concurrent requests" and "500 requests per second" test different things
and only the second one exercises a rate limiter meaningfully. Stages are
declared as (label, rps, seconds) so a ramp can be described directly:

    10 rps for 3s  ->  150 for 5s  ->  500 for 10s  ->  60 for 5s

Why this is multi-process
-------------------------
Measured on the dev machine: a single Python process using httpx tops out at
about 150 requests/second offered load -- the ceiling is per-request client
overhead, not the network and not the service under test. The mock PSPs, once
running on uvloop + httptools, comfortably absorbed 561 rps.

So a single-process generator cannot produce a 500 rps flash sale, and any
"we hit 500 rps" claim from one would be false. The generator therefore shards
the target rate across `--workers` OS processes and aggregates their results.
The load generator being the bottleneck is a property of the test harness, and
it is worth stating plainly rather than mistaking it for a limit of the router.

Usage:
    python3 -m loadgen.generate --profile spike
    python3 -m loadgen.generate --rps 500 --seconds 10 --workers 4
    python3 -m loadgen.generate --concurrent 500
"""

from __future__ import annotations

import argparse
import asyncio
import random
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field

import httpx
from concurrent.futures import ProcessPoolExecutor

ROUTER = "http://127.0.0.1:8080"

# Bounded, keep-alive connection pool per worker process.
#
# macOS caps the TCP listen backlog at kern.ipc.somaxconn = 128. An unbounded
# pool opens a fresh connection per request, overruns that backlog and the
# generator starts reporting ConnectError -- which looks like the service
# refusing traffic when it is really the loopback socket layer giving up. A
# bounded pool with keep-alive establishes its sockets once and reuses them,
# which is also what a real checkout service would do.
CLIENT_LIMITS = {"max_connections": 64, "max_keepalive_connections": 64}

# A shopper abandons checkout long before 15s. 8s is already generous and makes
# the generator's own timeouts visible instead of hiding them behind patience.
CLIENT_TIMEOUT_S = 8.0
PSPS = {
    "psp-atlas": "http://127.0.0.1:9001",
    "psp-borealis": "http://127.0.0.1:9002",
    "psp-cygnus": "http://127.0.0.1:9003",
}

# Mercado Luna sells electronics and fashion. Most baskets are small; a thin
# tail of high-value electronics orders is what priority routing is for.
def sample_amount_cents() -> tuple[int, bool]:
    roll = random.random()
    if roll < 0.70:            # fashion / accessories
        amount = random.randint(29900, 149900)
    elif roll < 0.95:          # mid-range electronics
        amount = random.randint(150000, 899900)
    else:                      # premium electronics
        amount = random.randint(900000, 4500000)
    return amount, amount >= 500000  # priority above 5,000 MXN


@dataclass
class Stage:
    label: str
    rps: int
    seconds: float


PROFILES: dict[str, list[Stage]] = {
    # The real Mercado Luna shape: nothing, then everything, then a long tail.
    "spike": [
        Stage("pre-sale", 10, 3),
        Stage("doors open", 150, 4),
        Stage("MIDNIGHT SPIKE", 500, 10),
        Stage("sustained", 300, 5),
        Stage("tail", 60, 4),
    ],
    "normal": [Stage("steady", 120, 12)],
    "overload": [Stage("beyond capacity", 700, 12)],
    "burst": [],  # handled by --concurrent
}


@dataclass
class Results:
    approved: int = 0
    declined: int = 0
    failed: int = 0
    shed: int = 0
    no_processor: int = 0
    other: int = 0
    by_processor: Counter = field(default_factory=Counter)
    errors: Counter = field(default_factory=Counter)
    latencies: list[float] = field(default_factory=list)
    queued: list[float] = field(default_factory=list)
    queued_normal: list[float] = field(default_factory=list)
    queued_priority: list[float] = field(default_factory=list)
    failover_count: int = 0
    approved_volume_cents: int = 0
    fees_cents: int = 0

    def record(self, status_code: int, body: dict) -> None:
        status = body.get("status")
        attempts = body.get("attempts") or []
        if len(attempts) > 1:
            self.failover_count += 1
        lat = body.get("total_latency_ms")
        if lat is not None:
            self.latencies.append(lat)
        q = body.get("queued_ms") or 0
        if q > 0:
            self.queued.append(q)
            # Tracked per lane: the two lanes have different configured
            # deadlines (2500ms / 5000ms), so a single max is unfalsifiable.
            if body.get("priority"):
                self.queued_priority.append(q)
            else:
                self.queued_normal.append(q)

        if status == "approved":
            self.approved += 1
            self.by_processor[body.get("processor_id", "?")] += 1
            self.approved_volume_cents += body.get("amount_cents") or 0
            self.fees_cents += body.get("fee_cents") or 0
        elif status == "declined":
            self.declined += 1
            self.by_processor[body.get("processor_id", "?")] += 1
        else:
            err = body.get("error", f"http_{status_code}")
            self.errors[err] += 1
            if err == "system_at_capacity":
                self.shed += 1
            elif err == "no_healthy_processor":
                self.no_processor += 1
            elif err == "all_processors_failed":
                self.failed += 1
            else:
                self.other += 1

    def to_dict(self) -> dict:
        return {
            "approved": self.approved,
            "declined": self.declined,
            "failed": self.failed,
            "shed": self.shed,
            "no_processor": self.no_processor,
            "other": self.other,
            "by_processor": dict(self.by_processor),
            "errors": dict(self.errors),
            "latencies": self.latencies,
            "queued": self.queued,
            "queued_normal": self.queued_normal,
            "queued_priority": self.queued_priority,
            "failover_count": self.failover_count,
            "approved_volume_cents": self.approved_volume_cents,
            "fees_cents": self.fees_cents,
        }

    @classmethod
    def merge(cls, parts: list[dict]) -> "Results":
        out = cls()
        for part in parts:
            out.approved += part["approved"]
            out.declined += part["declined"]
            out.failed += part["failed"]
            out.shed += part["shed"]
            out.no_processor += part["no_processor"]
            out.other += part["other"]
            out.by_processor.update(part["by_processor"])
            out.errors.update(part["errors"])
            out.latencies.extend(part["latencies"])
            out.queued.extend(part["queued"])
            out.queued_normal.extend(part["queued_normal"])
            out.queued_priority.extend(part["queued_priority"])
            out.failover_count += part["failover_count"]
            out.approved_volume_cents += part["approved_volume_cents"]
            out.fees_cents += part["fees_cents"]
        return out

    @property
    def total(self) -> int:
        return (
            self.approved + self.declined + self.failed
            + self.shed + self.no_processor + self.other
        )

    def pct(self, n: int) -> str:
        return f"{n / self.total * 100:5.1f}%" if self.total else "  n/a"

    def percentile(self, p: float) -> float | None:
        if not self.latencies:
            return None
        s = sorted(self.latencies)
        return round(s[min(len(s) - 1, int(len(s) * p))], 1)


async def _fire(client: httpx.AsyncClient, results: Results, live: Counter) -> None:
    amount, priority = sample_amount_cents()
    payload = {"amount_cents": amount, "currency": "MXN", "priority": priority}
    try:
        resp = await client.post("/v1/payments/authorize", json=payload)
        body = resp.json()
        body.setdefault("amount_cents", amount)
        results.record(resp.status_code, body)
        live[body.get("status") or body.get("error") or "unknown"] += 1
    except Exception as exc:
        results.other += 1
        results.errors[f"client_{type(exc).__name__}"] += 1
        live["client_error"] += 1


async def run_stages(stages: list[Stage], quiet: bool = False) -> Results:
    results = Results()
    tasks: set[asyncio.Task] = set()
    limits = httpx.Limits(**CLIENT_LIMITS)

    async with httpx.AsyncClient(base_url=ROUTER, timeout=CLIENT_TIMEOUT_S, limits=limits) as client:
        for stage in stages:
            live = Counter()
            loop = asyncio.get_running_loop()
            stage_start = loop.time()
            total = int(stage.rps * stage.seconds)
            ticks_per_sec = 50
            per_tick = max(1, round(stage.rps / ticks_per_sec))
            sent = 0
            last_report = stage_start

            if not quiet:
                print(
                    f"\n  [{stage.label}] target {stage.rps} rps for {stage.seconds:g}s "
                    f"({total} requests)"
                )

            while sent < total:
                batch = min(per_tick, total - sent)
                for _ in range(batch):
                    t = asyncio.create_task(_fire(client, results, live))
                    tasks.add(t)
                    t.add_done_callback(tasks.discard)
                sent += batch

                now = loop.time()
                if not quiet and now - last_report >= 1.0:
                    elapsed = now - stage_start
                    print(
                        f"    t+{elapsed:4.1f}s  sent={sent:<6} "
                        f"ok={live['approved']:<6} declined={live['declined']:<5} "
                        f"shed={live['system_at_capacity']:<5} "
                        f"failed={live['all_processors_failed'] + live['no_healthy_processor']:<5} "
                        f"inflight={len(tasks)}"
                    )
                    last_report = now

                target = stage_start + sent / stage.rps
                delay = target - loop.time()
                if delay > 0:
                    await asyncio.sleep(delay)

            if tasks:
                await asyncio.gather(*list(tasks), return_exceptions=True)

    return results


async def run_burst(concurrent: int, quiet: bool = False) -> Results:
    """Fire N requests with no pacing at all -- the literal reading of the brief's
    'send 500 concurrent requests'."""
    results = Results()
    live = Counter()
    limits = httpx.Limits(**CLIENT_LIMITS)
    if not quiet:
        print(f"\n  [burst] firing {concurrent} simultaneous requests with zero pacing")
    async with httpx.AsyncClient(base_url=ROUTER, timeout=CLIENT_TIMEOUT_S, limits=limits) as client:
        started = time.perf_counter()
        await asyncio.gather(
            *[_fire(client, results, live) for _ in range(concurrent)],
            return_exceptions=True,
        )
        if not quiet:
            print(f"    completed in {time.perf_counter() - started:.2f}s")
    return results


async def fetch_psp_stats() -> dict[str, dict]:
    out = {}
    async with httpx.AsyncClient(timeout=5.0) as client:
        for pid, url in PSPS.items():
            try:
                out[pid] = (await client.get(f"{url}/stats")).json()
            except Exception as exc:
                out[pid] = {"error": str(exc)}
    return out


async def fetch_router_metrics() -> dict:
    async with httpx.AsyncClient(timeout=5.0) as client:
        return (await client.get(f"{ROUTER}/metrics")).json()


def print_results(results: Results, title: str = "RESULTS") -> None:
    print(f"\n  {title}")
    print("  " + "-" * 66)
    print(f"    total requests      {results.total:>8}")
    print(f"    approved            {results.approved:>8}   {results.pct(results.approved)}")
    print(f"    declined by bank    {results.declined:>8}   {results.pct(results.declined)}")
    print(f"    shed (at capacity)  {results.shed:>8}   {results.pct(results.shed)}")
    print(f"    failed (technical)  {results.failed:>8}   {results.pct(results.failed)}")
    print(f"    no processor        {results.no_processor:>8}   {results.pct(results.no_processor)}")
    if results.other:
        print(f"    other               {results.other:>8}   {results.pct(results.other)}")
    handled = results.approved + results.declined
    if results.total:
        print(
            f"    handled by a PSP    {handled:>8}   {results.pct(handled)}"
            f"   <- checkout completed"
        )
    print(f"    failovers used      {results.failover_count:>8}")
    print(
        f"    latency             p50={results.percentile(0.5)}ms  "
        f"p95={results.percentile(0.95)}ms  p99={results.percentile(0.99)}ms"
    )
    if results.queued:
        avg_q = sum(results.queued) / len(results.queued)
        print(
            f"    queued              {len(results.queued)} requests waited, "
            f"avg {avg_q:.0f}ms, max {max(results.queued):.0f}ms"
        )
        for lane, waits, budget in (
            ("normal", results.queued_normal, 2500),
            ("priority", results.queued_priority, 5000),
        ):
            if waits:
                over = sum(1 for w in waits if w > budget)
                print(
                    f"      {lane:<9} n={len(waits):<6} max={max(waits):>6.0f}ms  "
                    f"budget={budget}ms  over budget: {over} "
                    f"({over / len(waits) * 100:.1f}%)"
                )
    if results.by_processor:
        print("\n    traffic distribution")
        served = sum(results.by_processor.values())
        for pid, n in results.by_processor.most_common():
            bar = "#" * int(n / served * 40) if served else ""
            print(f"      {pid:<16}{n:>7}  {n / served * 100:5.1f}%  {bar}")
    if results.errors:
        print("\n    error breakdown")
        for err, n in results.errors.most_common():
            print(f"      {err:<28}{n:>7}")


async def assert_rate_limits_respected() -> bool:
    """The core proof for Requirement 1, from the processors' own counters."""
    stats = await fetch_psp_stats()
    print("\n  RATE LIMIT VERIFICATION (reported by the processors themselves)")
    print("  " + "-" * 66)
    print(f"    {'processor':<16}{'limit':>7}{'peak rps':>10}{'429s':>7}{'received':>10}  verdict")
    all_ok = True
    for pid, s in stats.items():
        if "error" in s:
            print(f"    {pid:<16}  unreachable: {s['error']}")
            all_ok = False
            continue
        ok = s["rate_limit_rejections"] == 0 and s["peak_rps_observed"] <= s["max_rps"]
        all_ok = all_ok and ok
        print(
            f"    {pid:<16}{s['max_rps']:>7}{s['peak_rps_observed']:>10}"
            f"{s['rate_limit_rejections']:>7}{s['requests_received']:>10}"
            f"  {'PASS' if ok else 'FAIL'}"
        )
    print()
    print(
        "    VERDICT: "
        + (
            "PASS -- no processor was ever sent more than its configured limit"
            if all_ok
            else "FAIL -- a processor was overloaded"
        )
    )
    return all_ok


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Mercado Luna flash-sale traffic generator")
    p.add_argument("--profile", choices=sorted(PROFILES), default="spike")
    p.add_argument("--rps", type=int, help="override: constant rate")
    p.add_argument("--seconds", type=float, default=10, help="duration for --rps")
    p.add_argument("--concurrent", type=int, help="fire N unpaced simultaneous requests")
    p.add_argument(
        "--workers", type=int, default=4,
        help="OS processes generating load. One process caps near 150 rps.",
    )
    p.add_argument("--no-monitor", action="store_true", help="suppress the live router table")
    p.add_argument("--reset", action="store_true", help="reset router + PSP counters first")
    return p


async def reset_all() -> None:
    async with httpx.AsyncClient(timeout=5.0) as client:
        await client.post(f"{ROUTER}/admin/reset-metrics")
        for url in PSPS.values():
            await client.post(f"{url}/admin/reset")


async def main() -> None:
    args = build_parser().parse_args()
    if args.reset:
        await reset_all()

    if args.concurrent:
        print(f"\n  BURST: {args.concurrent} unpaced concurrent requests "
              f"across {args.workers} processes")
        results = await drive(None, args.workers, concurrent=args.concurrent,
                             monitor=not args.no_monitor)
        title = f"BURST RESULTS ({args.concurrent} concurrent)"
    elif args.rps:
        stages = [Stage("constant", args.rps, args.seconds)]
        print(f"\n  LOAD: {args.rps} rps for {args.seconds:g}s across {args.workers} processes")
        results = await drive(stages, args.workers, monitor=not args.no_monitor)
        title = f"RESULTS ({args.rps} rps for {args.seconds:g}s)"
    else:
        stages = PROFILES[args.profile]
        plan = " -> ".join(f"{s.rps}rps/{s.seconds:g}s" for s in stages)
        print(f"\n  PROFILE '{args.profile}' across {args.workers} processes: {plan}")
        results = await drive(stages, args.workers, monitor=not args.no_monitor)
        title = f"RESULTS (profile: {args.profile})"

    print_results(results, title)
    await assert_rate_limits_respected()




# ---------------------------------------------------------------------------
# Multi-process driver
# ---------------------------------------------------------------------------

def _worker_stages(stages_spec: list[tuple[str, int, float]]) -> dict:
    """Entry point in a child process. Must be module-level to be picklable."""
    import uvloop  # noqa: PLC0415 -- child-process-local import

    uvloop.install()
    stages = [Stage(label, rps, secs) for label, rps, secs in stages_spec]
    return asyncio.run(run_stages(stages, quiet=True)).to_dict()


def _worker_burst(n: int) -> dict:
    import uvloop  # noqa: PLC0415

    uvloop.install()
    return asyncio.run(run_burst(n, quiet=True)).to_dict()


def shard_stages(stages: list[Stage], workers: int) -> list[list[tuple[str, int, float]]]:
    """Split each stage's target rate across workers, distributing the remainder."""
    shards: list[list[tuple[str, int, float]]] = [[] for _ in range(workers)]
    for stage in stages:
        base, extra = divmod(stage.rps, workers)
        for w in range(workers):
            rps = base + (1 if w < extra else 0)
            if rps > 0:
                shards[w].append((stage.label, rps, stage.seconds))
    return shards


async def live_monitor(stop: asyncio.Event, interval: float = 1.0) -> None:
    """Print the ROUTER's own view of the world while load is running.

    This is the observability requirement in action: the numbers below come from
    the service under test, not from the load generator, so they show what the
    router believes about capacity and processor health in real time.
    """
    header = (
        f"    {'t':>5}  {'in/s':>5} {'ok':>7} {'decl':>6} {'shed':>6} {'fail':>6} "
        f"{'q':>5}  {'atlas':>11} {'borealis':>11} {'cygnus':>11}"
    )
    print(header)
    print("    " + "-" * (len(header) - 4))
    t0 = time.perf_counter()
    # A generous timeout: under saturation the router's own event loop is busy
    # serving payments, and /metrics rightly queues behind them. A 3s timeout
    # here silently produced an empty monitor table -- a check that reads
    # nothing and reports nothing is worse than no check, so failures are now
    # printed rather than swallowed.
    async with httpx.AsyncClient(base_url=ROUTER, timeout=10.0) as client:
        while not stop.is_set():
            try:
                m = (await client.get("/metrics")).json()
                t, q = m["traffic"], m["queue"]
                cells = []
                for pid in ("psp-atlas", "psp-borealis", "psp-cygnus"):
                    p = next(x for x in m["processors"] if x["processor_id"] == pid)
                    state = p["health"]["breaker_state"]
                    mark = {"closed": "", "open": "!", "half_open": "?"}[state]
                    cells.append(
                        f"{p['capacity']['current_rps']:>3}/"
                        f"{p['capacity']['effective_limit_rps']:<3}{mark:<4}"
                    )
                print(
                    f"    {time.perf_counter() - t0:5.1f}  {t['incoming_rps']:>5} "
                    f"{t['approved']:>7} {t['declined']:>6} "
                    f"{t['rejected_system_at_capacity']:>6} "
                    f"{t['failed_technical'] + t['rejected_no_healthy_processor']:>6} "
                    f"{q['normal_queued'] + q['priority_queued']:>5}  "
                    + " ".join(f"{c:>11}" for c in cells)
                )
            except Exception as exc:
                print(f"    {time.perf_counter() - t0:5.1f}  <metrics unavailable: "
                      f"{type(exc).__name__}>")
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass
    print("    (! = circuit open, ? = half-open probing)")


async def drive(
    stages: list[Stage] | None,
    workers: int,
    concurrent: int | None = None,
    monitor: bool = True,
) -> Results:
    """Run load across `workers` processes with a live router monitor."""
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    monitor_task = asyncio.create_task(live_monitor(stop)) if monitor else None

    with ProcessPoolExecutor(max_workers=workers) as pool:
        if concurrent is not None:
            base, extra = divmod(concurrent, workers)
            futures = [
                loop.run_in_executor(pool, _worker_burst, base + (1 if w < extra else 0))
                for w in range(workers)
                if base + (1 if w < extra else 0) > 0
            ]
        else:
            futures = [
                loop.run_in_executor(pool, _worker_stages, shard)
                for shard in shard_stages(stages or [], workers)
                if shard
            ]
        parts = await asyncio.gather(*futures)

    stop.set()
    if monitor_task:
        await monitor_task
    return Results.merge(list(parts))


if __name__ == "__main__":
    asyncio.run(main())
