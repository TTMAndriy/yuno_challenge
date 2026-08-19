"""Health tracking and the circuit breaker. Tests for Requirement 2.

The first test is the domain decision the whole service rests on.
"""

import pytest

from router.health import BreakerState, HealthTier, Outcome, ProcessorHealth


def make(**kw) -> ProcessorHealth:
    defaults = dict(
        processor_id="psp-test",
        window_seconds=60,
        min_samples=20,
        technical_success_threshold=0.60,
        auth_rate_floor=0.30,
        open_cooldown_seconds=30,
        probe_requests=2,
    )
    defaults.update(kw)
    return ProcessorHealth(**defaults)


def test_declines_never_open_the_breaker():
    """A bank saying no is not a processor failing.

    A midnight flash sale produces a wave of insufficient-funds declines. If
    those tripped the breaker, the healthiest processor would be removed from
    rotation at the worst possible moment.
    """
    h = make()
    for _ in range(500):
        h.record(Outcome.DECLINED)
    assert h.state is BreakerState.CLOSED
    assert h.allows_traffic() is True
    assert h.technical_success_rate() == 1.0


def test_collapsed_auth_rate_degrades_but_does_not_open():
    h = make()
    for _ in range(100):
        h.record(Outcome.DECLINED)
    assert h.tier() is HealthTier.DEGRADED
    assert h.state is BreakerState.CLOSED


def test_technical_errors_open_the_breaker():
    h = make()
    for _ in range(100):
        h.record(Outcome.TECHNICAL_ERROR)
    assert h.state is BreakerState.OPEN
    assert h.allows_traffic() is False
    assert h.tier() is HealthTier.UNAVAILABLE


def test_min_samples_prevents_an_early_false_positive():
    """Two unlucky failures at second one must not kill a healthy processor."""
    h = make(min_samples=20)
    for _ in range(5):
        h.record(Outcome.TECHNICAL_ERROR)
    assert h.state is BreakerState.CLOSED
    assert h.sample_count() == 5


def test_undefined_rate_is_none_not_zero():
    """A processor with no traffic has no success rate. Zero would be a lie."""
    h = make()
    assert h.technical_success_rate() is None
    assert h.auth_rate() is None
    assert h.tier() is HealthTier.HEALTHY


def test_full_open_probe_recover_cycle():
    h = make(open_cooldown_seconds=0, probe_requests=2)
    for _ in range(100):
        h.record(Outcome.TECHNICAL_ERROR)
    assert h.state is BreakerState.OPEN

    assert h.wants_probe() is True            # cooldown of 0 promotes to HALF_OPEN
    assert h.state is BreakerState.HALF_OPEN
    assert h.allows_traffic() is False        # probes only, never real traffic

    assert h.claim_probe() is True
    h.record(Outcome.APPROVED, is_probe=True)
    assert h.claim_probe() is True
    h.record(Outcome.APPROVED, is_probe=True)

    assert h.state is BreakerState.CLOSED
    assert h.allows_traffic() is True


def test_failed_probe_reopens_the_breaker():
    h = make(open_cooldown_seconds=0, probe_requests=2)
    for _ in range(100):
        h.record(Outcome.TECHNICAL_ERROR)
    h.wants_probe()
    for _ in range(2):
        h.claim_probe()
        h.record(Outcome.TECHNICAL_ERROR, is_probe=True)
    assert h.state is BreakerState.OPEN


def test_probe_slots_are_bounded():
    h = make(open_cooldown_seconds=0, probe_requests=2)
    for _ in range(100):
        h.record(Outcome.TECHNICAL_ERROR)
    h.wants_probe()
    assert h.claim_probe() is True
    assert h.claim_probe() is True
    assert h.claim_probe() is False   # never a third


def test_cooldown_is_respected_before_probing():
    h = make(open_cooldown_seconds=30)
    for _ in range(100):
        h.record(Outcome.TECHNICAL_ERROR)
    assert h.state is BreakerState.OPEN
    assert h.wants_probe() is False   # 30s has not passed


def test_recovery_clears_the_window():
    """A stale failure burst must not immediately re-open a recovered breaker."""
    h = make(open_cooldown_seconds=0, probe_requests=1)
    for _ in range(100):
        h.record(Outcome.TECHNICAL_ERROR)
    h.wants_probe()
    h.claim_probe()
    h.record(Outcome.APPROVED, is_probe=True)
    assert h.state is BreakerState.CLOSED
    assert h.sample_count() == 0
    h.record(Outcome.APPROVED)
    assert h.state is BreakerState.CLOSED


def test_state_changes_are_recorded_with_a_reason():
    h = make()
    for _ in range(100):
        h.record(Outcome.TECHNICAL_ERROR)
    assert h.state_changes
    last = h.state_changes[-1]
    assert last["to"] == "open"
    assert "technical_success_rate" in last["reason"]
