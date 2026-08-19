"""Orchestration: admission control, dispatch, failover, recovery probes."""

from __future__ import annotations

import asyncio
import time
import uuid
from collections import deque

from .config import AppConfig
from .health import Outcome
from .logging_setup import get_logger
from .models import AttemptRecord, AuthorizationRequest, AuthorizationResponse
from .pool import Processor, ProcessorPool
from .upstream import UpstreamClient

log = get_logger("router")


class Counters:
    def __init__(self) -> None:
        self.started_at = time.time()
        self.received = 0
        self.approved = 0
        self.declined = 0
        self.failed_technical = 0
        self.rejected_at_capacity = 0
        self.rejected_queue_timeout = 0
        self.rejected_no_processor = 0
        self.failovers = 0
        self.upstream_429s = 0
        self.probes_sent = 0
        self.queued_count = 0
        self.queue_wait_ms_total = 0.0
        self.latency_samples: deque[float] = deque(maxlen=5000)
        self.arrivals: deque[float] = deque()

    def observe_arrival(self, now: float) -> None:
        self.arrivals.append(now)
        cutoff = now - 1.0
        while self.arrivals and self.arrivals[0] <= cutoff:
            self.arrivals.popleft()

    def incoming_rps(self) -> int:
        cutoff = time.monotonic() - 1.0
        while self.arrivals and self.arrivals[0] <= cutoff:
            self.arrivals.popleft()
        return len(self.arrivals)

    def percentile(self, pct: float) -> float | None:
        if not self.latency_samples:
            return None
        ordered = sorted(self.latency_samples)
        idx = min(len(ordered) - 1, int(len(ordered) * pct))
        return round(ordered[idx], 2)


class LoadDistributionService:
    def __init__(self, cfg: AppConfig) -> None:
        self.cfg = cfg
        self.pool = ProcessorPool(cfg)
        self.client = UpstreamClient(
            cfg.upstream.connect_timeout_s, cfg.upstream.read_timeout_s
        )
        self.counters = Counters()
        # Admission control: two lanes, separate depth ceilings.
        #
        # `_queued` counts requests currently *blocked waiting for capacity*.
        # `_inflight` counts everything in service, including requests already
        # dispatched to a processor and awaiting its response.
        # These are different numbers and conflating them breaks load shedding:
        # shedding must trigger on the backlog, not on healthy concurrency.
        self._queued = {"normal": 0, "priority": 0}
        self._inflight = {"normal": 0, "priority": 0}
        self._probe_task: asyncio.Task | None = None
        self._log_every = 25  # sample routing logs; see note in main.py

    # -- lifecycle ---------------------------------------------------------

    async def start(self) -> None:
        self._probe_task = asyncio.create_task(self._probe_loop())
        log.info(
            "router ready | strategy=%s | capacity=%d rps across %d processors | %s",
            self.cfg.routing_strategy,
            self.cfg.total_capacity_rps,
            len(self.pool.processors),
            ", ".join(
                f"{p.id}={p.limiter.effective_limit}/{p.cfg.max_rps}rps@{p.fee_percent}%"
                for p in self.pool.processors
            ),
        )

    async def stop(self) -> None:
        if self._probe_task:
            self._probe_task.cancel()
            try:
                await self._probe_task
            except asyncio.CancelledError:
                pass
        await self.client.aclose()

    # -- recovery probes ---------------------------------------------------

    async def _probe_loop(self) -> None:
        """Synthetic health probes for OPEN breakers.

        Deliberately synthetic rather than borrowing real customer traffic: a
        processor with an OPEN breaker is known to be failing, so sending a real
        shopper's payment there to find out whether it recovered would burn a
        genuine checkout to gather telemetry. A tiny synthetic authorisation
        costs nothing and tells us the same thing.
        """
        while True:
            try:
                await asyncio.sleep(1.0)
                for processor in self.pool.probe_candidates():
                    if not processor.health.claim_probe():
                        continue
                    if not processor.limiter.try_acquire():
                        continue
                    self.counters.probes_sent += 1
                    key = f"probe-{uuid.uuid4()}"
                    try:
                        result = await self.client.authorize(
                            processor.base_url, key, 100, "MXN", None, False
                        )
                    finally:
                        processor.limiter.release()
                    processor.health.record(result.outcome, is_probe=True)
                    log.info(
                        "PROBE  %-14s outcome=%-16s status=%s latency=%.0fms",
                        processor.id,
                        result.outcome.value,
                        result.http_status,
                        result.latency_ms,
                    )
                    self._flush_state_changes()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # never let the probe loop die
                log.warning("probe loop error: %s", exc)

    def _flush_state_changes(self) -> None:
        for change in self.pool.drain_state_changes():
            symbol = "RECOVERED" if change["to"] == "closed" else "CIRCUIT"
            log.warning(
                "%s  %-14s %s -> %s | %s",
                symbol,
                change["processor_id"],
                change["from"].upper(),
                change["to"].upper(),
                change["reason"],
            )

    # -- the request path --------------------------------------------------

    async def authorize(self, req: AuthorizationRequest) -> tuple[int, AuthorizationResponse]:
        request_id = str(uuid.uuid4())
        idem_key = req.idempotency_key or f"ml-{request_id}"
        lane = "priority" if req.priority else "normal"
        started = time.perf_counter()
        now = time.monotonic()

        self.counters.received += 1
        self.counters.observe_arrival(now)

        max_depth = (
            self.cfg.admission.max_priority_queue_depth
            if req.priority
            else self.cfg.admission.max_queue_depth
        )
        timeout_s = (
            self.cfg.admission.priority_queue_timeout_ms
            if req.priority
            else self.cfg.admission.queue_timeout_ms
        ) / 1000.0

        # --- admission control: shed load before it becomes latency -------
        if self._queued[lane] >= max_depth:
            self.counters.rejected_at_capacity += 1
            log.warning(
                "SHED   lane=%s queue_depth=%d >= %d | rejecting request_id=%s",
                lane, self._queued[lane], max_depth, request_id,
            )
            return 503, AuthorizationResponse(
                status="failed",
                request_id=request_id,
                idempotency_key=idem_key,
                error="system_at_capacity",
                message=(
                    f"All processors saturated and the {lane} queue is full "
                    f"({max_depth} waiting). Retry shortly."
                ),
                total_latency_ms=(time.perf_counter() - started) * 1000,
            )

        attempts: list[AttemptRecord] = []
        tried: set[str] = set()
        queued_ms = 0.0

        self._inflight[lane] += 1
        try:
            for attempt_no in range(1, self.cfg.upstream.max_attempts + 1):
                processor, waited_ms = await self._acquire_processor(
                    tried, req.priority, timeout_s, lane
                )
                queued_ms += waited_ms

                if processor is None:
                    if not self.pool.any_available(exclude=tried):
                        # Every processor is either open-circuited or exhausted.
                        if attempts:
                            break  # fall through to the failure response below
                        self.counters.rejected_no_processor += 1
                        return 503, AuthorizationResponse(
                            status="failed",
                            request_id=request_id,
                            idempotency_key=idem_key,
                            error="no_healthy_processor",
                            message="Every configured processor is currently unavailable.",
                            queued_ms=round(queued_ms, 2),
                            total_latency_ms=(time.perf_counter() - started) * 1000,
                            attempts=attempts,
                        )
                    if attempts:
                        break
                    self.counters.rejected_queue_timeout += 1
                    return 503, AuthorizationResponse(
                        status="failed",
                        request_id=request_id,
                        idempotency_key=idem_key,
                        error="system_at_capacity",
                        message=(
                            f"No processor capacity within {timeout_s * 1000:.0f}ms. "
                            "Retry shortly."
                        ),
                        queued_ms=round(queued_ms, 2),
                        total_latency_ms=(time.perf_counter() - started) * 1000,
                    )

                tried.add(processor.id)
                try:
                    result = await self.client.authorize(
                        processor.base_url,
                        idem_key,
                        req.amount_cents,
                        req.currency,
                        req.card_bin,
                        req.priority,
                    )
                finally:
                    # The in-flight slot must come back on every path, including
                    # cancellation, or the processor silently bleeds capacity.
                    processor.limiter.release()
                processor.health.record(result.outcome)
                self._flush_state_changes()

                attempts.append(
                    AttemptRecord(
                        processor_id=processor.id,
                        outcome=result.outcome.value,
                        http_status=result.http_status,
                        latency_ms=round(result.latency_ms, 2),
                        retryable=result.retryable,
                    )
                )

                if result.rate_limited:
                    # Should never happen. If it does, our limiter is mis-sized.
                    self.counters.upstream_429s += 1
                    log.error(
                        "RATE-LIMIT BREACH %-14s returned 429 (observed_rps=%s, max=%s). "
                        "Our limiter admitted more than the processor accepts.",
                        processor.id,
                        result.body.get("observed_rps"),
                        result.body.get("max_rps"),
                    )

                if result.outcome is Outcome.APPROVED:
                    processor.record_approval_economics(req.amount_cents)
                    self.counters.approved += 1
                    total_ms = (time.perf_counter() - started) * 1000
                    self.counters.latency_samples.append(total_ms)
                    if queued_ms > 0:
                        self.counters.queued_count += 1
                        self.counters.queue_wait_ms_total += queued_ms
                    self._maybe_log_route(processor, "APPROVED", req, attempts, queued_ms, total_ms)
                    return 200, AuthorizationResponse(
                        status="approved",
                        request_id=request_id,
                        idempotency_key=idem_key,
                        processor_id=processor.id,
                        processor_name=processor.name,
                        authorization_code=result.body.get("authorization_code"),
                        fee_percent=processor.fee_percent,
                        fee_cents=result.body.get("fee_cents"),
                        queued_ms=round(queued_ms, 2),
                        total_latency_ms=round(total_ms, 2),
                        attempts=attempts,
                    )

                if result.outcome is Outcome.DECLINED:
                    self.counters.declined += 1
                    total_ms = (time.perf_counter() - started) * 1000
                    self.counters.latency_samples.append(total_ms)
                    self._maybe_log_route(processor, "DECLINED", req, attempts, queued_ms, total_ms)
                    return 200, AuthorizationResponse(
                        status="declined",
                        request_id=request_id,
                        idempotency_key=idem_key,
                        processor_id=processor.id,
                        processor_name=processor.name,
                        decline_code=result.body.get("decline_code"),
                        decline_reason=result.body.get("decline_reason"),
                        queued_ms=round(queued_ms, 2),
                        total_latency_ms=round(total_ms, 2),
                        attempts=attempts,
                    )

                # Technical error from here on.
                if not result.retryable or attempt_no >= self.cfg.upstream.max_attempts:
                    break
                self.counters.failovers += 1
                log.info(
                    "FAILOVER %-14s -> retrying elsewhere (outcome=%s status=%s) key=%s",
                    processor.id, result.outcome.value, result.http_status, idem_key,
                )
        finally:
            self._inflight[lane] -= 1

        self.counters.failed_technical += 1
        total_ms = (time.perf_counter() - started) * 1000
        return 502, AuthorizationResponse(
            status="failed",
            request_id=request_id,
            idempotency_key=idem_key,
            error="all_processors_failed",
            message=(
                f"Tried {len(attempts)} processor(s); none returned an authorisation."
            ),
            queued_ms=round(queued_ms, 2),
            total_latency_ms=round(total_ms, 2),
            attempts=attempts,
        )

    async def _acquire_processor(
        self, tried: set[str], priority: bool, timeout_s: float, lane: str
    ) -> tuple[Processor | None, float]:
        """Select a processor, waiting briefly for capacity if necessary.

        This is the "queue". Rather than a separate queue object plus a worker
        pool, each in-flight request parks itself here until a capacity slot
        frees up or its deadline expires. The waiting count *is* the queue depth,
        and it is what admission control sheds against. Fewer moving parts, and
        no risk of a worker pool and a queue disagreeing about who owns a request.
        """
        poll_s = self.cfg.admission.poll_interval_ms / 1000.0
        deadline = time.monotonic() + timeout_s
        waited_start = time.perf_counter()

        processor = self.pool.select(exclude=tried, priority=priority)
        if processor is not None:
            return processor, 0.0

        # From here on the request is genuinely queued: it wants a processor and
        # none has capacity. This is the window that admission control sheds on.
        self._queued[lane] += 1
        try:
            while time.monotonic() < deadline:
                await asyncio.sleep(poll_s)
                processor = self.pool.select(exclude=tried, priority=priority)
                if processor is not None:
                    return processor, (time.perf_counter() - waited_start) * 1000
                if not self.pool.any_available(exclude=tried):
                    return None, (time.perf_counter() - waited_start) * 1000
            return None, (time.perf_counter() - waited_start) * 1000
        finally:
            self._queued[lane] -= 1

    def _maybe_log_route(
        self,
        processor: Processor,
        outcome: str,
        req: AuthorizationRequest,
        attempts: list[AttemptRecord],
        queued_ms: float,
        total_ms: float,
    ) -> None:
        """Sampled per-request logging.

        At 500 rps, logging every routing decision produces an unreadable wall of
        text and the log writer itself becomes a bottleneck -- the observability
        would actively damage the thing being observed. So routine decisions are
        sampled (1 in N) while everything that matters -- circuit state changes,
        failovers, load shedding, rate-limit breaches -- is logged unconditionally.
        """
        n = self.counters.received
        if n % self._log_every != 0 and not req.priority and len(attempts) == 1:
            return
        log.info(
            "ROUTE  %-14s %-9s %8.2f MXN%s attempts=%d queued=%.0fms total=%.0fms",
            processor.id,
            outcome,
            req.amount_cents / 100,
            " [PRIORITY]" if req.priority else "",
            len(attempts),
            queued_ms,
            total_ms,
        )

    # -- metrics -----------------------------------------------------------

    def metrics(self) -> dict:
        c = self.counters
        handled = c.approved + c.declined
        return {
            "service": "smart-load-distribution",
            "uptime_s": round(time.time() - c.started_at, 1),
            "routing_strategy": self.cfg.routing_strategy,
            "traffic": {
                "requests_received": c.received,
                "incoming_rps": c.incoming_rps(),
                "total_capacity_rps": self.cfg.total_capacity_rps,
                "effective_capacity_rps": sum(
                    p.limiter.effective_limit for p in self.pool.processors
                ),
                "approved": c.approved,
                "declined": c.declined,
                "failed_technical": c.failed_technical,
                "rejected_system_at_capacity": c.rejected_at_capacity + c.rejected_queue_timeout,
                "rejected_no_healthy_processor": c.rejected_no_processor,
                "approval_rate_of_handled": round(c.approved / handled, 4) if handled else None,
                "handled_pct_of_received": round(handled / c.received, 4) if c.received else None,
            },
            "reliability": {
                "failovers": c.failovers,
                "recovery_probes_sent": c.probes_sent,
                "upstream_rate_limit_breaches": c.upstream_429s,
                "rate_limit_respected": c.upstream_429s == 0,
            },
            "queue": {
                "normal_queued": self._queued["normal"],
                "priority_queued": self._queued["priority"],
                "normal_inflight": self._inflight["normal"],
                "priority_inflight": self._inflight["priority"],
                "max_queue_depth": self.cfg.admission.max_queue_depth,
                "max_priority_queue_depth": self.cfg.admission.max_priority_queue_depth,
                "queue_timeout_ms": self.cfg.admission.queue_timeout_ms,
                "requests_that_queued": c.queued_count,
                "avg_queue_wait_ms": (
                    round(c.queue_wait_ms_total / c.queued_count, 2) if c.queued_count else 0.0
                ),
            },
            "latency_ms": {
                "p50": c.percentile(0.50),
                "p95": c.percentile(0.95),
                "p99": c.percentile(0.99),
                "samples": len(c.latency_samples),
            },
            "cost": self.pool.cost_report(),
            "processors": self.pool.snapshot(),
        }
