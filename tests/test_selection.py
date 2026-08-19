"""The selection rule: which processor gets the next request, and why."""

from router.config import (
    AdmissionConfig,
    AppConfig,
    HealthConfig,
    ProcessorConfig,
    UpstreamConfig,
)
from router.health import Outcome
from router.pool import ProcessorPool

PROCS = (
    ProcessorConfig("psp-atlas", "Atlas", "http://a", 150, 2.5, baseline=True),
    ProcessorConfig("psp-borealis", "Borealis", "http://b", 100, 2.9),
    ProcessorConfig("psp-cygnus", "Cygnus", "http://c", 200, 2.1),
)


def cfg(strategy: str = "cost_aware") -> AppConfig:
    return AppConfig(
        routing_strategy=strategy,
        health=HealthConfig(min_samples=20, open_cooldown_seconds=30),
        admission=AdmissionConfig(),
        upstream=UpstreamConfig(rate_limit_headroom=0.85, inflight_ratio=0.15),
        processors=PROCS,
    )


def test_cost_aware_prefers_the_cheapest_healthy_processor():
    pool = ProcessorPool(cfg("cost_aware"))
    p = pool.select()
    assert p.id == "psp-cygnus"      # 2.1%, the cheapest
    assert p.fee_percent == 2.1


def test_traffic_spills_to_the_next_cheapest_when_the_first_is_full():
    """Under load every processor is used; that is Requirement 1's distribution."""
    pool = ProcessorPool(cfg("cost_aware"))
    picked = {}
    for _ in range(2000):
        p = pool.select()
        key = p.id if p else "NONE"
        picked[key] = picked.get(key, 0) + 1
        if p:
            p.limiter.release()      # release in-flight so the rate ceiling binds
    assert set(picked) >= {"psp-atlas", "psp-borealis", "psp-cygnus"}
    for p in pool.processors:
        assert picked[p.id] == p.limiter.effective_limit


def test_no_processor_is_ever_over_admitted():
    pool = ProcessorPool(cfg())
    for _ in range(5000):
        p = pool.select()
        if p:
            p.limiter.release()
    for p in pool.processors:
        assert p.limiter.current_rps() <= p.limiter.effective_limit
        assert p.limiter.effective_limit + p.limiter.max_inflight <= p.cfg.max_rps


def test_open_breaker_removes_a_processor_from_selection():
    pool = ProcessorPool(cfg())
    cygnus = pool.get("psp-cygnus")
    for _ in range(100):
        cygnus.health.record(Outcome.TECHNICAL_ERROR)
    seen = set()
    for _ in range(300):
        p = pool.select()
        if p:
            seen.add(p.id)
            p.limiter.release()
    assert "psp-cygnus" not in seen
    assert seen == {"psp-atlas", "psp-borealis"}


def test_exclude_prevents_retrying_the_same_processor():
    """Failover must land somewhere else or it is not failover."""
    pool = ProcessorPool(cfg())
    first = pool.select()
    second = pool.select(exclude={first.id})
    assert second is not None
    assert second.id != first.id


def test_priority_ignores_cost_and_takes_the_most_reliable():
    pool = ProcessorPool(cfg("cost_aware"))
    normal = pool.select(priority=False)
    assert normal.id == "psp-cygnus"          # cheapest
    priority = pool.select(priority=True)
    assert priority.fee_percent >= normal.fee_percent   # reliability, not fee


def test_balanced_strategy_spreads_by_utilisation():
    pool = ProcessorPool(cfg("balanced"))
    picked = {}
    for _ in range(30):
        p = pool.select()
        picked[p.id] = picked.get(p.id, 0) + 1
        p.limiter.release()
    assert len(picked) == 3                    # all three warm, not just the cheapest


def test_any_available_reports_no_healthy_processor():
    pool = ProcessorPool(cfg())
    assert pool.any_available() is True
    for p in pool.processors:
        for _ in range(100):
            p.health.record(Outcome.TECHNICAL_ERROR)
    assert pool.any_available() is False
    assert pool.select() is None


def test_cost_report_separates_the_lanes():
    """A single blended figure reported negative savings and looked broken."""
    pool = ProcessorPool(cfg())
    cygnus, atlas = pool.get("psp-cygnus"), pool.get("psp-atlas")
    cygnus.record_approval_economics(1_000_000, priority=False)   # cheap lane
    atlas.record_approval_economics(1_000_000, priority=True)     # priority lane
    rep = pool.cost_report()
    assert rep["normal_lane"]["savings_cents"] > 0        # 2.1% beats a 2.5% baseline
    assert rep["priority_lane"]["savings_cents"] == 0     # atlas IS the baseline
    assert rep["combined"]["approved_volume_cents"] == 2_000_000
