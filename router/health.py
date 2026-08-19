"""Rolling health windows and the per-processor circuit breaker.

Domain note: a decline is not a failure
---------------------------------------
The brief says "track each processor's recent success rate ... if it drops below
a threshold, stop sending traffic to it". Taken literally that is wrong for
payments, and getting it right is the single most important domain decision in
this service.

A card declined for insufficient funds, a fraud block, or an expired card is a
*correct* response from a perfectly healthy processor. It means the issuing bank
said no. If we let declines trip the circuit breaker, then a wave of genuine
declines -- which is exactly what a midnight flash sale produces, as shoppers
hammer maxed-out cards -- would take our healthiest processor out of rotation
and make the outage worse.

So we track two distinct signals:

  technical_success_rate = (approved + declined) / total
      "Did the processor answer us correctly at all?"
      Timeouts, 5xx, connection errors and 429s are the failures here.
      THIS drives the circuit breaker. It is what "processor unhealthy" means.

  auth_rate = approved / (approved + declined)
      "Of the requests the processor handled, how many did banks approve?"
      Reported for observability and used as a *soft* signal: a collapsed auth
      rate deprioritises a processor but never opens the breaker, because the
      cause is usually upstream of the PSP (a BIN range, an issuer outage) and
      cutting the processor off would not help.

Minimum sample size
-------------------
A processor with zero requests has an undefined success rate. Worse, a processor
that has served two requests and failed both has a 0% rate that means nothing.
The breaker will not open until `min_samples` observations exist in the window,
which stops two unlucky failures at second one from killing a healthy processor.
"""

from __future__ import annotations

import time
from collections import deque
from enum import Enum


class Outcome(str, Enum):
    APPROVED = "approved"
    DECLINED = "declined"
    TECHNICAL_ERROR = "technical_error"


class BreakerState(str, Enum):
    CLOSED = "closed"        # healthy, taking normal traffic
    OPEN = "open"            # removed from rotation, probes only
    HALF_OPEN = "half_open"  # probing for recovery


class HealthTier(int, Enum):
    """Routing preference. Higher is better; compared before cost."""
    HEALTHY = 2
    DEGRADED = 1   # answering fine, but auth rate has collapsed
    UNAVAILABLE = 0


class ProcessorHealth:
    def __init__(
        self,
        processor_id: str,
        window_seconds: int = 60,
        min_samples: int = 20,
        technical_success_threshold: float = 0.60,
        auth_rate_floor: float = 0.30,
        open_cooldown_seconds: int = 30,
        probe_requests: int = 2,
    ) -> None:
        self.processor_id = processor_id
        self.window_seconds = window_seconds
        self.min_samples = min_samples
        self.technical_success_threshold = technical_success_threshold
        self.auth_rate_floor = auth_rate_floor
        self.open_cooldown_seconds = open_cooldown_seconds
        self.probe_requests = probe_requests

        self._events: deque[tuple[float, Outcome]] = deque()
        self.state = BreakerState.CLOSED
        self._opened_at: float | None = None
        self._probes_issued = 0
        self._probe_results: list[Outcome] = []

        # lifetime counters (never pruned) for the metrics endpoint
        self.total_requests = 0
        self.total_approved = 0
        self.total_declined = 0
        self.total_technical_errors = 0
        self.state_changes: list[dict] = []

    # -- window bookkeeping ------------------------------------------------

    def _prune(self, now: float) -> None:
        cutoff = now - self.window_seconds
        while self._events and self._events[0][0] <= cutoff:
            self._events.popleft()

    def _counts(self, now: float) -> tuple[int, int, int]:
        self._prune(now)
        approved = declined = errors = 0
        for _, outcome in self._events:
            if outcome is Outcome.APPROVED:
                approved += 1
            elif outcome is Outcome.DECLINED:
                declined += 1
            else:
                errors += 1
        return approved, declined, errors

    # -- rates -------------------------------------------------------------

    def technical_success_rate(self, now: float | None = None) -> float | None:
        now = time.monotonic() if now is None else now
        approved, declined, errors = self._counts(now)
        total = approved + declined + errors
        if total == 0:
            return None  # undefined, NOT zero
        return (approved + declined) / total

    def auth_rate(self, now: float | None = None) -> float | None:
        now = time.monotonic() if now is None else now
        approved, declined, _ = self._counts(now)
        handled = approved + declined
        if handled == 0:
            return None
        return approved / handled

    def sample_count(self, now: float | None = None) -> int:
        now = time.monotonic() if now is None else now
        self._prune(now)
        return len(self._events)

    # -- state machine -----------------------------------------------------

    def _transition(self, new_state: BreakerState, reason: str, now: float) -> None:
        if new_state is self.state:
            return
        old = self.state
        self.state = new_state
        entry = {
            "at": time.time(),
            "processor_id": self.processor_id,
            "from": old.value,
            "to": new_state.value,
            "reason": reason,
        }
        self.state_changes.append(entry)
        self._pending_log = entry  # picked up by the caller for logging

        if new_state is BreakerState.OPEN:
            self._opened_at = now
            self._probes_issued = 0
            self._probe_results = []
        elif new_state is BreakerState.HALF_OPEN:
            self._probes_issued = 0
            self._probe_results = []
        elif new_state is BreakerState.CLOSED:
            self._opened_at = None
            # Clear the window so a stale failure burst cannot immediately
            # re-open the breaker on the next request.
            self._events.clear()

    def take_pending_log(self) -> dict | None:
        entry = getattr(self, "_pending_log", None)
        self._pending_log = None
        return entry

    def record(self, outcome: Outcome, is_probe: bool = False, now: float | None = None) -> None:
        now = time.monotonic() if now is None else now
        self.total_requests += 1
        if outcome is Outcome.APPROVED:
            self.total_approved += 1
        elif outcome is Outcome.DECLINED:
            self.total_declined += 1
        else:
            self.total_technical_errors += 1

        if is_probe and self.state is BreakerState.HALF_OPEN:
            self._probe_results.append(outcome)
            healthy_probes = sum(
                1 for o in self._probe_results if o is not Outcome.TECHNICAL_ERROR
            )
            if len(self._probe_results) >= self.probe_requests:
                ratio = healthy_probes / len(self._probe_results)
                if ratio >= self.technical_success_threshold:
                    self._transition(
                        BreakerState.CLOSED,
                        f"recovered: {healthy_probes}/{len(self._probe_results)} probes healthy",
                        now,
                    )
                else:
                    self._transition(
                        BreakerState.OPEN,
                        f"probe failed: {healthy_probes}/{len(self._probe_results)} probes healthy",
                        now,
                    )
            return

        self._events.append((now, outcome))
        self._evaluate(now)

    def _evaluate(self, now: float) -> None:
        if self.state is not BreakerState.CLOSED:
            return
        samples = self.sample_count(now)
        if samples < self.min_samples:
            return
        rate = self.technical_success_rate(now)
        if rate is not None and rate < self.technical_success_threshold:
            self._transition(
                BreakerState.OPEN,
                f"technical_success_rate {rate:.1%} < {self.technical_success_threshold:.0%} "
                f"over {samples} samples",
                now,
            )

    # -- admission ---------------------------------------------------------

    def allows_traffic(self, now: float | None = None) -> bool:
        """True if this processor may take normal (non-probe) traffic."""
        now = time.monotonic() if now is None else now
        if self.state is BreakerState.CLOSED:
            return True
        if self.state is BreakerState.OPEN:
            if self._opened_at is not None and now - self._opened_at >= self.open_cooldown_seconds:
                self._transition(BreakerState.HALF_OPEN, "cooldown elapsed, probing", now)
            return False
        return False  # HALF_OPEN takes probes only

    def wants_probe(self, now: float | None = None) -> bool:
        now = time.monotonic() if now is None else now
        self.allows_traffic(now)  # may promote OPEN -> HALF_OPEN
        if self.state is not BreakerState.HALF_OPEN:
            return False
        return self._probes_issued < self.probe_requests

    def claim_probe(self) -> bool:
        """Synchronous reservation of a probe slot -- no await, no race."""
        if self.state is not BreakerState.HALF_OPEN:
            return False
        if self._probes_issued >= self.probe_requests:
            return False
        self._probes_issued += 1
        return True

    def tier(self, now: float | None = None) -> HealthTier:
        now = time.monotonic() if now is None else now
        if not self.allows_traffic(now):
            return HealthTier.UNAVAILABLE
        ar = self.auth_rate(now)
        if ar is not None and self.sample_count(now) >= self.min_samples and ar < self.auth_rate_floor:
            return HealthTier.DEGRADED
        return HealthTier.HEALTHY

    def snapshot(self) -> dict:
        now = time.monotonic()
        approved, declined, errors = self._counts(now)
        tsr = self.technical_success_rate(now)
        ar = self.auth_rate(now)
        return {
            "breaker_state": self.state.value,
            "health_tier": self.tier(now).name.lower(),
            "window_seconds": self.window_seconds,
            "window_samples": approved + declined + errors,
            "window_approved": approved,
            "window_declined": declined,
            "window_technical_errors": errors,
            "technical_success_rate": None if tsr is None else round(tsr, 4),
            "auth_rate": None if ar is None else round(ar, 4),
            "min_samples_to_trip": self.min_samples,
            "lifetime": {
                "requests": self.total_requests,
                "approved": self.total_approved,
                "declined": self.total_declined,
                "technical_errors": self.total_technical_errors,
            },
            "state_changes": self.state_changes[-10:],
        }
