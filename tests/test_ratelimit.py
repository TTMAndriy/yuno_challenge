"""The rate limiter. These are the tests for Requirement 1.

The invariant test is the important one: it encodes the bug that took three
attempts to find, so a future edit to the headroom constants cannot silently
reintroduce it.
"""

import time

import pytest

from router.ratelimit import SlidingWindowLimiter


@pytest.mark.parametrize("max_rps", [2, 3, 7, 100, 150, 200, 1000])
@pytest.mark.parametrize("headroom,ratio", [(0.75, 0.25), (0.85, 0.15), (0.95, 0.5), (1.0, 1.0)])
def test_invariant_holds_for_every_configuration(max_rps, headroom, ratio):
    """effective_limit + max_inflight must never exceed max_rps.

    The two ceilings can sum: a full in-flight batch can land inside the same
    upstream second as a full rate window. Cygnus was measured at 210 arrivals
    against a 200 limit with 170 rate + 42 in-flight. This must be impossible to
    configure, not merely avoided by choosing good constants.
    """
    limiter = SlidingWindowLimiter(max_rps, headroom, ratio)
    assert limiter.effective_limit + limiter.max_inflight <= max_rps
    assert limiter.effective_limit >= 1
    assert limiter.max_inflight >= 1


def test_adversarial_config_is_clamped_not_accepted():
    limiter = SlidingWindowLimiter(100, headroom=0.95, inflight_ratio=0.5)
    assert limiter.inflight_was_clamped is True
    assert limiter.effective_limit + limiter.max_inflight <= 100


@pytest.mark.parametrize("bad", [0, 1, -5])
def test_rejects_capacity_that_cannot_be_protected(bad):
    """Below 2 rps the invariant is unsatisfiable, so refuse rather than pretend."""
    with pytest.raises(ValueError):
        SlidingWindowLimiter(bad)


def test_inflight_bound_binds_before_rate_bound():
    """With nothing released, in-flight is the binding constraint."""
    limiter = SlidingWindowLimiter(150, 0.85, 0.15)
    admitted = sum(1 for _ in range(1000) if limiter.try_acquire())
    assert admitted == limiter.max_inflight
    assert limiter.has_capacity() is False


def test_release_returns_capacity():
    limiter = SlidingWindowLimiter(150, 0.85, 0.15)
    for _ in range(limiter.max_inflight):
        assert limiter.try_acquire()
    assert limiter.try_acquire() is False
    limiter.release()
    assert limiter.try_acquire() is True


def test_release_below_zero_is_safe():
    """Double release must not create phantom capacity."""
    limiter = SlidingWindowLimiter(100, 0.85, 0.15)
    limiter.release()
    limiter.release()
    limiter.try_acquire()
    limiter.release()
    limiter.release()
    admitted = sum(1 for _ in range(1000) if limiter.try_acquire())
    assert admitted <= limiter.max_inflight


def test_rate_ceiling_enforced_within_one_window():
    """With in-flight always released, the rate window is what binds."""
    limiter = SlidingWindowLimiter(150, 0.85, 0.15)
    now = time.monotonic()
    admitted = 0
    for _ in range(1000):
        if limiter.try_acquire(now):
            admitted += 1
            limiter.release()
    assert admitted == limiter.effective_limit


def test_window_slides():
    limiter = SlidingWindowLimiter(12, 1.0, 1.0)
    t0 = 1000.0
    for _ in range(limiter.effective_limit):
        assert limiter.try_acquire(t0)
        limiter.release()
    assert limiter.try_acquire(t0) is False
    # One second and a hair later the window has emptied.
    assert limiter.try_acquire(t0 + 1.001) is True


def test_try_acquire_is_synchronous():
    """No await between check and mutation -- that is the whole race story.

    If try_acquire were a coroutine, two callers could both observe the last
    free slot before either decremented it. Asserting it is a plain function
    keeps that guarantee from being refactored away.
    """
    import inspect

    assert not inspect.iscoroutinefunction(SlidingWindowLimiter.try_acquire)
    assert not inspect.iscoroutinefunction(SlidingWindowLimiter.release)
    assert not inspect.iscoroutinefunction(SlidingWindowLimiter.has_capacity)


def test_snapshot_exposes_the_guarantee():
    limiter = SlidingWindowLimiter(200, 0.85, 0.15)
    snap = limiter.snapshot()
    assert snap["worst_case_arrivals"] <= snap["max_rps"]
    for key in ("current_rps", "max_inflight", "peak_rps_observed", "utilization_pct"):
        assert key in snap
