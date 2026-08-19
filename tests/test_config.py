"""The shipped configuration must satisfy the guarantees the README claims."""

from router.config import load_config
from router.pool import ProcessorPool


def test_shipped_config_loads():
    cfg = load_config()
    assert len(cfg.processors) >= 3
    assert cfg.routing_strategy in {"cost_aware", "balanced"}


def test_shipped_config_satisfies_the_rate_limit_invariant():
    """The guarantee has to hold for the config we actually ship, not just in theory."""
    pool = ProcessorPool(load_config())
    for p in pool.processors:
        worst_case = p.limiter.effective_limit + p.limiter.max_inflight
        assert worst_case <= p.cfg.max_rps, (
            f"{p.id}: worst-case arrivals {worst_case} exceeds its limit {p.cfg.max_rps}"
        )


def test_queue_is_sized_to_the_latency_budget_not_to_memory():
    """A queue far deeper than one second of capacity guarantees timeouts."""
    cfg = load_config()
    pool = ProcessorPool(cfg)
    capacity = sum(p.limiter.effective_limit for p in pool.processors)
    seconds_of_backlog = cfg.admission.max_queue_depth / capacity
    assert seconds_of_backlog < 1.5, (
        f"queue holds {seconds_of_backlog:.1f}s of work; shedding early beats queueing deep"
    )


# Measured p95 round trip is 34ms when the load generator is not competing with
# the service for CPU. 100ms is used here for margin. An earlier version of this
# constant read 250ms, taken from a run where six generator workers and four
# service processes shared eight cores -- that figure measured scheduler
# contention, not the processors.
REFERENCE_LATENCY_S = 0.10


def test_inflight_bound_does_not_throttle_below_the_rate_ceiling():
    """The in-flight bound must not become the throughput ceiling.

    By Little's Law a bound of N slots caps throughput at N / latency. Sized at
    15% of the rate ceiling it did exactly that: Atlas pinned at 19/19 in-flight
    while using 67 of its 127 rps, with in-flight rejections outnumbering rate
    rejections 20,439 to 833. Satisfying the safety invariant is not enough; the
    capacity also has to be reachable.
    """
    pool = ProcessorPool(load_config())
    for p in pool.processors:
        achievable = p.limiter.max_inflight / REFERENCE_LATENCY_S
        assert achievable >= p.limiter.effective_limit * 0.95, (
            f"{p.id}: in-flight bound of {p.limiter.max_inflight} caps throughput at "
            f"{achievable:.0f} rps, below its {p.limiter.effective_limit} rps rate "
            f"ceiling -- the bound is throttling, not protecting"
        )


def test_usable_capacity_is_reported_honestly():
    """Both ceilings reserved inside max_rps means advertised != usable."""
    cfg = load_config()
    pool = ProcessorPool(cfg)
    advertised = sum(p.cfg.max_rps for p in pool.processors)
    usable = sum(
        min(p.limiter.effective_limit, p.limiter.max_inflight / REFERENCE_LATENCY_S)
        for p in pool.processors
    )
    assert usable < advertised          # the guarantee costs something
    assert usable > advertised * 0.7    # but not most of the capacity


def test_a_baseline_processor_is_identified_for_cost_reporting():
    cfg = load_config()
    assert cfg.baseline_processor() is not None


def test_every_processor_has_a_distinct_identity():
    cfg = load_config()
    assert len({p.id for p in cfg.processors}) == len(cfg.processors)
    assert len({p.base_url for p in cfg.processors}) == len(cfg.processors)
