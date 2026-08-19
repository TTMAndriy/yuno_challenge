"""The orchestration layer: admission control, failover, retry, capacity release.

This is the largest module and every real bug found during development lived
here -- queued-vs-inflight conflation, the per-attempt deadline, the in-flight
release path. It had no tests until now, which is the wrong way round: the most
bug-prone code was the least covered.

The upstream client is stubbed so these run with no network and no mock PSPs.
"""

import asyncio

import pytest

from router.config import (
    AdmissionConfig,
    AppConfig,
    HealthConfig,
    ProcessorConfig,
    UpstreamConfig,
)
from router.health import Outcome
from router.models import AuthorizationRequest
from router.service import LoadDistributionService
from router.upstream import UpstreamResult

PROCS = (
    ProcessorConfig("psp-atlas", "Atlas", "http://a", 150, 2.5, baseline=True),
    ProcessorConfig("psp-borealis", "Borealis", "http://b", 100, 2.9),
    ProcessorConfig("psp-cygnus", "Cygnus", "http://c", 200, 2.1),
)


def cfg(**admission) -> AppConfig:
    return AppConfig(
        routing_strategy="cost_aware",
        health=HealthConfig(min_samples=20, open_cooldown_seconds=30),
        admission=AdmissionConfig(**admission),
        upstream=UpstreamConfig(
            rate_limit_headroom=0.85, inflight_ratio=0.15, max_attempts=2
        ),
        processors=PROCS,
    )


class FakeUpstream:
    """Returns a scripted outcome per processor and records what it was sent."""

    def __init__(self, outcomes: dict[str, Outcome], latency_s: float = 0.0):
        self.outcomes = outcomes
        self.latency_s = latency_s
        self.calls: list[dict] = []

    async def authorize(self, base_url, idempotency_key, amount_cents, currency, card_bin, priority):
        pid = {"http://a": "psp-atlas", "http://b": "psp-borealis", "http://c": "psp-cygnus"}[base_url]
        self.calls.append({"processor_id": pid, "idempotency_key": idempotency_key,
                           "amount_cents": amount_cents, "priority": priority})
        if self.latency_s:
            await asyncio.sleep(self.latency_s)
        outcome = self.outcomes.get(pid, Outcome.APPROVED)
        if outcome is Outcome.APPROVED:
            return UpstreamResult(outcome, False, 200, 1.0,
                                  {"status": "approved", "authorization_code": "OK1", "fee_cents": 10})
        if outcome is Outcome.DECLINED:
            return UpstreamResult(outcome, False, 200, 1.0,
                                  {"status": "declined", "decline_code": "51",
                                   "decline_reason": "insufficient_funds"})
        return UpstreamResult(outcome, True, 503, 1.0, {"error": "processor_unavailable"})

    async def aclose(self):
        pass


def build(outcomes=None, latency_s=0.0, **admission):
    svc = LoadDistributionService(cfg(**admission))
    svc.client = FakeUpstream(outcomes or {}, latency_s)
    return svc


def req(**kw) -> AuthorizationRequest:
    return AuthorizationRequest(**{"amount_cents": 50000, "currency": "MXN", **kw})


# ------------------------------------------------------------ the happy paths

@pytest.mark.asyncio
async def test_approved_request_returns_200_and_names_its_processor():
    svc = build()
    status, body = await svc.authorize(req())
    assert status == 200
    assert body.status == "approved"
    assert body.processor_id == "psp-cygnus"          # cheapest healthy
    assert body.authorization_code == "OK1"
    assert len(body.attempts) == 1


@pytest.mark.asyncio
async def test_decline_is_returned_not_retried():
    """A bank decline must not trigger failover -- that is the retry policy."""
    svc = build({"psp-cygnus": Outcome.DECLINED})
    status, body = await svc.authorize(req())
    assert status == 200
    assert body.status == "declined"
    assert body.decline_code == "51"
    assert len(body.attempts) == 1                    # exactly one attempt
    assert len(svc.client.calls) == 1


# --------------------------------------------------------------- failover

@pytest.mark.asyncio
async def test_technical_error_fails_over_to_a_different_processor():
    svc = build({"psp-cygnus": Outcome.TECHNICAL_ERROR})
    status, body = await svc.authorize(req())
    assert status == 200
    assert body.status == "approved"
    assert len(body.attempts) == 2
    assert body.attempts[0].processor_id == "psp-cygnus"
    assert body.attempts[1].processor_id != "psp-cygnus"
    assert svc.counters.failovers == 1


@pytest.mark.asyncio
async def test_idempotency_key_is_identical_across_the_failover_hop():
    """Different keys on a retry is how a customer gets charged twice."""
    svc = build({"psp-cygnus": Outcome.TECHNICAL_ERROR})
    _, body = await svc.authorize(req(idempotency_key="order-777"))
    keys = {c["idempotency_key"] for c in svc.client.calls}
    assert keys == {"order-777"}
    assert len(svc.client.calls) == 2
    assert body.idempotency_key == "order-777"


@pytest.mark.asyncio
async def test_retry_budget_is_respected():
    """max_attempts=2 means two processors tried, never all three."""
    svc = build({p.id: Outcome.TECHNICAL_ERROR for p in PROCS})
    status, body = await svc.authorize(req())
    assert status == 502
    assert body.error == "all_processors_failed"
    assert len(body.attempts) == 2
    assert len(svc.client.calls) == 2


@pytest.mark.asyncio
async def test_no_healthy_processor_returns_503_not_502():
    svc = build()
    for p in svc.pool.processors:
        for _ in range(100):
            p.health.record(Outcome.TECHNICAL_ERROR)
    status, body = await svc.authorize(req())
    assert status == 503
    assert body.error == "no_healthy_processor"
    assert svc.counters.rejected_no_processor == 1


# ------------------------------------------------- capacity is never leaked

@pytest.mark.asyncio
async def test_inflight_slot_is_released_after_success():
    """If release leaked, throughput would collapse to max_inflight forever."""
    svc = build()
    total_inflight = sum(p.limiter.max_inflight for p in svc.pool.processors)
    for _ in range(total_inflight * 3):
        status, _ = await svc.authorize(req())
        assert status == 200
    for p in svc.pool.processors:
        assert p.limiter.snapshot()["inflight"] == 0


@pytest.mark.asyncio
async def test_inflight_slot_is_released_when_the_upstream_raises():
    """The finally block is the only thing standing between a bug and a leak."""
    svc = build()

    class Exploding(FakeUpstream):
        async def authorize(self, *a, **kw):
            raise RuntimeError("boom")

    svc.client = Exploding({})
    with pytest.raises(RuntimeError):
        await svc.authorize(req())
    assert all(p.limiter.snapshot()["inflight"] == 0 for p in svc.pool.processors)


# --------------------------------------------------------- admission control

@pytest.mark.asyncio
async def test_queue_full_sheds_immediately_with_system_at_capacity():
    svc = build(max_queue_depth=2, queue_timeout_ms=200)
    # Occupy every capacity slot so nothing can be selected.
    for p in svc.pool.processors:
        while p.limiter.try_acquire():
            pass
    # Two requests fill the queue; the third must be shed instantly.
    waiters = [asyncio.create_task(svc.authorize(req())) for _ in range(2)]
    await asyncio.sleep(0.05)
    assert svc._queued["normal"] == 2

    status, body = await svc.authorize(req())
    assert status == 503
    assert body.error == "system_at_capacity"
    assert svc.counters.rejected_at_capacity == 1
    await asyncio.gather(*waiters)


@pytest.mark.asyncio
async def test_queued_and_inflight_are_distinct_counters():
    """Conflating them meant shedding never triggered. Regression guard."""
    svc = build(queue_timeout_ms=300)
    for p in svc.pool.processors:
        while p.limiter.try_acquire():
            pass
    task = asyncio.create_task(svc.authorize(req()))
    await asyncio.sleep(0.05)
    assert svc._queued["normal"] == 1        # waiting for capacity
    assert svc._inflight["normal"] == 1      # also in service
    await task


@pytest.mark.asyncio
async def test_priority_lane_has_its_own_admission_budget():
    svc = build(max_queue_depth=1, max_priority_queue_depth=1, queue_timeout_ms=200)
    for p in svc.pool.processors:
        while p.limiter.try_acquire():
            pass
    normal = asyncio.create_task(svc.authorize(req()))
    await asyncio.sleep(0.03)
    # The normal lane is full, but the priority lane is untouched.
    prio = asyncio.create_task(svc.authorize(req(priority=True, amount_cents=900000)))
    await asyncio.sleep(0.03)
    assert svc._queued["priority"] == 1
    status, body = await svc.authorize(req())      # normal lane, still full
    assert body.error == "system_at_capacity"
    await asyncio.gather(normal, prio)


# ------------------------------------------------------- the latency budget

@pytest.mark.asyncio
async def test_deadline_is_request_scoped_not_per_attempt():
    """A retried request must not get a second full timeout.

    This is the bug that let a 2500ms budget produce a 5194ms wait: each attempt
    called _acquire_processor with a fresh deadline, so a failover doubled it.
    """
    budget_ms = 250
    svc = build(queue_timeout_ms=budget_ms)
    for p in svc.pool.processors:
        while p.limiter.try_acquire():
            pass

    loop = asyncio.get_running_loop()
    started = loop.time()
    status, body = await svc.authorize(req())
    elapsed_ms = (loop.time() - started) * 1000

    assert status == 503
    # Allow generous slack for scheduling, but nowhere near 2x the budget.
    assert elapsed_ms < budget_ms * 1.8, f"took {elapsed_ms:.0f}ms on a {budget_ms}ms budget"
    assert body.queued_ms <= elapsed_ms + 1


@pytest.mark.asyncio
async def test_capacity_freed_by_another_request_is_picked_up_while_waiting():
    """A queued request must actually be served when a slot frees.

    Independent of HOW waiting is implemented (polling or event-driven), so this
    test survives that refactor and is the safety net for it.
    """
    svc = build(queue_timeout_ms=2000)
    hog = []
    for p in svc.pool.processors:
        while p.limiter.try_acquire():
            hog.append(p)
    assert svc.pool.select() is None

    waiter = asyncio.create_task(svc.authorize(req()))
    await asyncio.sleep(0.05)
    assert svc._queued["normal"] == 1

    hog[0].limiter.release()                  # free exactly one slot
    status, body = await asyncio.wait_for(waiter, timeout=3.0)
    assert status == 200
    assert body.status == "approved"
    assert body.queued_ms > 0                 # it really did wait


# ------------------------------------------------------------------ metrics

@pytest.mark.asyncio
async def test_metrics_exposes_the_graded_signals():
    svc = build()
    await svc.authorize(req())
    m = svc.metrics()
    assert m["reliability"]["rate_limit_respected"] is True
    assert m["traffic"]["requests_received"] == 1
    assert len(m["processors"]) == 3
    for p in m["processors"]:
        assert p["capacity"]["worst_case_arrivals"] <= p["capacity"]["max_rps"]
        assert "breaker_state" in p["health"]
    assert "normal_queued" in m["queue"] and "normal_inflight" in m["queue"]
    assert "normal_lane" in m["cost"] and "priority_lane" in m["cost"]


@pytest.mark.asyncio
async def test_priority_flag_is_echoed_so_the_lane_is_observable():
    svc = build()
    _, body = await svc.authorize(req(priority=True, amount_cents=900000))
    assert body.priority is True
    _, body = await svc.authorize(req())
    assert body.priority is False
