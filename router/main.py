"""Smart Load Distribution Service -- HTTP surface.

    POST /v1/payments/authorize   the payment path
    GET  /metrics                 full live system state (Stretch Goal C)
    GET  /metrics/summary         one-screen human-readable digest
    GET  /healthz                 liveness + per-processor breaker states
    POST /admin/reset-metrics     zero the counters between demo scenarios

Run: uvicorn router.main:app --port 8080
"""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse, PlainTextResponse

from .config import load_config
from .logging_setup import get_logger
from .models import AuthorizationRequest
from .service import Counters, LoadDistributionService

log = get_logger("router.api")
config = load_config()
service = LoadDistributionService(config)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await service.start()
    yield
    await service.stop()


app = FastAPI(
    title="Mercado Luna -- Smart Load Distribution Service",
    description=(
        "Payment orchestration layer that distributes authorization traffic "
        "across multiple PSPs, respects each processor's rate limit, and fails "
        "over automatically when a processor degrades."
    ),
    version="1.0.0",
    lifespan=lifespan,
)


@app.post("/v1/payments/authorize")
async def authorize(req: AuthorizationRequest) -> JSONResponse:
    status_code, body = await service.authorize(req)
    return JSONResponse(status_code=status_code, content=body.model_dump(exclude_none=True))


@app.get("/metrics")
async def metrics() -> JSONResponse:
    return JSONResponse(content=service.metrics())


@app.get("/metrics/summary", response_class=PlainTextResponse)
async def metrics_summary() -> str:
    m = service.metrics()
    t, r, q = m["traffic"], m["reliability"], m["queue"]
    lines = [
        "MERCADO LUNA -- SMART LOAD DISTRIBUTION",
        f"strategy={m['routing_strategy']}  uptime={m['uptime_s']}s  "
        f"incoming={t['incoming_rps']} rps  capacity={t['effective_capacity_rps']} rps",
        "",
        f"received {t['requests_received']:>8}   approved {t['approved']:>8}   "
        f"declined {t['declined']:>8}",
        f"failed   {t['failed_technical']:>8}   shed     {t['rejected_system_at_capacity']:>8}   "
        f"no-psp   {t['rejected_no_healthy_processor']:>8}",
        f"failovers {r['failovers']:>7}   probes   {r['recovery_probes_sent']:>8}   "
        f"429-breaches {r['upstream_rate_limit_breaches']:>4}",
        f"queue     queued={q['normal_queued']}+{q['priority_queued']} "
        f"inflight={q['normal_inflight']}+{q['priority_inflight']} "
        f"queued_total={q['requests_that_queued']} avg_wait={q['avg_queue_wait_ms']}ms",
        f"latency   p50={m['latency_ms']['p50']}ms p95={m['latency_ms']['p95']}ms "
        f"p99={m['latency_ms']['p99']}ms",
        "",
        f"{'processor':<16}{'state':<12}{'tier':<11}{'rps':>10}{'peak':>7}"
        f"{'tech-ok':>10}{'auth':>8}{'fee':>7}{'reqs':>9}",
        "-" * 90,
    ]
    for p in m["processors"]:
        cap, h = p["capacity"], p["health"]
        tsr = "n/a" if h["technical_success_rate"] is None else f"{h['technical_success_rate']:.0%}"
        ar = "n/a" if h["auth_rate"] is None else f"{h['auth_rate']:.0%}"
        lines.append(
            f"{p['processor_id']:<16}{h['breaker_state']:<12}{h['health_tier']:<11}"
            f"{str(cap['current_rps']) + '/' + str(cap['effective_limit_rps']):>10}"
            f"{cap['peak_rps_observed']:>7}{tsr:>10}{ar:>8}"
            f"{str(p['fee_percent']) + '%':>7}{h['lifetime']['requests']:>9}"
        )
    c = m["cost"]
    n, pr = c["normal_lane"], c["priority_lane"]
    lines += [
        "",
        f"cost  normal lane   paid {n['fees_paid_cents'] / 100:>12,.2f} vs "
        f"{n['fees_if_all_baseline_cents'] / 100:>12,.2f} all-baseline"
        f"  -> saved {n['savings_cents'] / 100:>10,.2f} MXN "
        f"({n['savings_pct_of_fees']}%)  blended {n['blended_effective_fee_percent']}%",
        f"      priority lane paid {pr['fees_paid_cents'] / 100:>12,.2f} vs "
        f"{pr['fees_if_all_baseline_cents'] / 100:>12,.2f} all-baseline"
        f"  -> premium {-(pr['savings_cents'] or 0) / 100:>8,.2f} MXN "
        f"(routed for reliability, not fee)",
    ]
    return "\n".join(lines)


@app.get("/healthz")
async def healthz() -> dict:
    return {
        "ok": True,
        "processors": {
            p.id: {
                "breaker": p.health.state.value,
                "tier": p.health.tier().name.lower(),
                "current_rps": p.limiter.current_rps(),
            }
            for p in service.pool.processors
        },
    }


@app.post("/admin/reset-metrics")
async def reset_metrics() -> dict:
    """Zero the counters so each demo scenario reports in isolation.

    Deliberately does NOT reset circuit-breaker state or health windows -- those
    represent live beliefs about the processors and clearing them would hide the
    very behaviour the demo is meant to show.
    """
    service.counters = Counters()
    for p in service.pool.processors:
        p.approved_amount_cents = 0
        p.fees_paid_cents = 0
        p.priority_amount_cents = 0
        p.priority_fees_cents = 0
    log.info("metrics counters reset (breaker state and health windows preserved)")
    return {"reset": True}
