"""The processor pool: capacity + health per processor, and the selection rule.

Selection is the heart of the service. It runs on every request and must be
cheap, deterministic, and free of races.

    1. Drop processors whose circuit breaker is not CLOSED.
    2. Sort the survivors by preference.
    3. Walk the sorted list calling the *synchronous* `try_acquire()` until one
       succeeds. The first success wins.

Step 3 is why there is no `await` anywhere in `select()`. If we filtered on
"has capacity" and then awaited before reserving the slot, two coroutines could
both observe the last free slot and both dispatch -- over-admitting the upstream
and triggering exactly the 429s this service exists to prevent. Reserving inside
the same synchronous call makes that impossible under asyncio, without a lock.

Preference ordering
-------------------
`cost_aware` (default):   health tier desc, then fee asc, then utilisation asc
`balanced`:               health tier desc, then utilisation asc, then fee asc

cost_aware sends everything to the cheapest healthy processor until it is full,
then spills to the next cheapest. Under flash-sale load every processor
saturates and traffic ends up distributed in proportion to capacity anyway, so
we get Stretch Goal A's cost optimisation for free without sacrificing
distribution when it matters. At low volume it *will* concentrate on the cheapest
processor -- that is the point of the strategy, and `balanced` is the one-line
config flip for anyone who would rather keep all three warm.

Priority traffic ignores cost entirely and takes the healthiest processor with
the most headroom, per Stretch Goal B.
"""

from __future__ import annotations

import time

from .config import AppConfig, ProcessorConfig
from .health import BreakerState, HealthTier, Outcome, ProcessorHealth
from .ratelimit import SlidingWindowLimiter


class Processor:
    def __init__(self, cfg: ProcessorConfig, app_cfg: AppConfig) -> None:
        self.cfg = cfg
        self.id = cfg.id
        self.name = cfg.name
        self.base_url = cfg.base_url
        self.fee_percent = cfg.fee_percent
        self.limiter = SlidingWindowLimiter(
            cfg.max_rps,
            headroom=app_cfg.upstream.rate_limit_headroom,
            inflight_ratio=app_cfg.upstream.inflight_ratio,
        )
        self.health = ProcessorHealth(
            processor_id=cfg.id,
            window_seconds=app_cfg.health.window_seconds,
            min_samples=app_cfg.health.min_samples,
            technical_success_threshold=app_cfg.health.technical_success_threshold,
            auth_rate_floor=app_cfg.health.auth_rate_floor,
            open_cooldown_seconds=app_cfg.health.open_cooldown_seconds,
            probe_requests=app_cfg.health.probe_requests,
        )
        # Accounting for the cost-savings report (Stretch Goal A). Split by lane
        # because the two lanes optimise for different things and blending them
        # makes the report unreadable -- see ProcessorPool.cost_report.
        self.approved_amount_cents = 0
        self.fees_paid_cents = 0
        self.priority_amount_cents = 0
        self.priority_fees_cents = 0

    def record_approval_economics(self, amount_cents: int, priority: bool = False) -> None:
        fee = round(amount_cents * self.fee_percent / 100)
        self.approved_amount_cents += amount_cents
        self.fees_paid_cents += fee
        if priority:
            self.priority_amount_cents += amount_cents
            self.priority_fees_cents += fee

    def snapshot(self) -> dict:
        return {
            "processor_id": self.id,
            "processor_name": self.name,
            "base_url": self.base_url,
            "fee_percent": self.fee_percent,
            "capacity": self.limiter.snapshot(),
            "health": self.health.snapshot(),
            "economics": {
                "approved_volume_cents": self.approved_amount_cents,
                "fees_paid_cents": self.fees_paid_cents,
            },
        }


class ProcessorPool:
    def __init__(self, app_cfg: AppConfig) -> None:
        self.cfg = app_cfg
        self.processors: list[Processor] = [Processor(p, app_cfg) for p in app_cfg.processors]
        self._by_id = {p.id: p for p in self.processors}
        self.baseline_fee_percent = app_cfg.baseline_processor().fee_percent

    def get(self, processor_id: str) -> Processor | None:
        return self._by_id.get(processor_id)

    # -- selection ---------------------------------------------------------

    def _sort_key(self, p: Processor, now: float, priority: bool):
        tier = p.health.tier(now).value
        util = p.limiter.utilization(now)
        if priority or self.cfg.routing_strategy == "balanced":
            # Reliability and headroom first; cost is the tiebreak.
            return (-tier, util, p.fee_percent)
        return (-tier, p.fee_percent, util)

    def select(
        self,
        exclude: set[str] | None = None,
        priority: bool = False,
        now: float | None = None,
    ) -> Processor | None:
        """Pick a processor AND reserve one capacity slot on it. Fully sync."""
        now = time.monotonic() if now is None else now
        exclude = exclude or set()

        candidates = [
            p
            for p in self.processors
            if p.id not in exclude and p.health.allows_traffic(now)
        ]
        if not candidates:
            return None

        candidates.sort(key=lambda p: self._sort_key(p, now, priority))
        for p in candidates:
            if p.limiter.try_acquire(now):
                return p
        return None

    def any_available(self, exclude: set[str] | None = None, now: float | None = None) -> bool:
        """Is at least one processor admitting traffic (ignoring capacity)?"""
        now = time.monotonic() if now is None else now
        exclude = exclude or set()
        return any(
            p.health.allows_traffic(now) for p in self.processors if p.id not in exclude
        )

    def probe_candidates(self, now: float | None = None) -> list[Processor]:
        now = time.monotonic() if now is None else now
        return [p for p in self.processors if p.health.wants_probe(now)]

    # -- reporting ---------------------------------------------------------

    def drain_state_changes(self) -> list[dict]:
        changes = []
        for p in self.processors:
            entry = p.health.take_pending_log()
            if entry:
                changes.append(entry)
        return changes

    def cost_report(self) -> dict:
        """Stretch Goal A: savings versus routing everything to the baseline.

        Reported per lane, because a single blended number was actively
        misleading. Priority traffic is routed by reliability, not fee, and
        priority traffic is by definition the high-value baskets -- so it lands
        disproportionately on the *expensive* reliable processors and carries
        disproportionate volume. Blending the lanes produced a headline of
        "saved -1,581 MXN", i.e. cost optimisation appearing to lose money when
        it was in fact working and being deliberately overridden for the orders
        that matter most.

        Splitting them shows both truths: normal traffic saves money, and
        priority routing knowingly pays a premium to protect large orders.
        """
        total_volume = sum(p.approved_amount_cents for p in self.processors)
        total_fees = sum(p.fees_paid_cents for p in self.processors)
        prio_volume = sum(p.priority_amount_cents for p in self.processors)
        prio_fees = sum(p.priority_fees_cents for p in self.processors)
        norm_volume = total_volume - prio_volume
        norm_fees = total_fees - prio_fees

        def lane(volume: int, fees: int) -> dict:
            baseline = round(volume * self.baseline_fee_percent / 100)
            return {
                "approved_volume_cents": volume,
                "fees_paid_cents": fees,
                "fees_if_all_baseline_cents": baseline,
                "savings_cents": baseline - fees,
                "savings_pct_of_fees": (
                    round((baseline - fees) / baseline * 100, 2) if baseline else None
                ),
                "blended_effective_fee_percent": (
                    round(fees / volume * 100, 4) if volume else None
                ),
            }

        return {
            "baseline_processor_fee_percent": self.baseline_fee_percent,
            "note": (
                "Normal-lane savings is the Stretch Goal A number. The priority "
                "lane routes by reliability rather than fee, so a premium there "
                "is the intended tradeoff, not a regression."
            ),
            "normal_lane": lane(norm_volume, norm_fees),
            "priority_lane": lane(prio_volume, prio_fees),
            "combined": lane(total_volume, total_fees),
        }

    def snapshot(self) -> list[dict]:
        return [p.snapshot() for p in self.processors]
