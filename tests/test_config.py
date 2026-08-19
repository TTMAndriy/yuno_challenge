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


def test_a_baseline_processor_is_identified_for_cost_reporting():
    cfg = load_config()
    assert cfg.baseline_processor() is not None


def test_every_processor_has_a_distinct_identity():
    cfg = load_config()
    assert len({p.id for p in cfg.processors}) == len(cfg.processors)
    assert len({p.base_url for p in cfg.processors}) == len(cfg.processors)
