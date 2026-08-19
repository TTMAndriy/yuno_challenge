"""A mock Payment Service Provider.

Three of these run as independent processes on different ports. Each one:

  * enforces its OWN rate limit over a trailing one-second window and returns
    HTTP 429 when exceeded. This is deliberate and it is what makes Requirement
    1 provable rather than merely asserted: the router claims it never exceeds a
    processor's capacity, and the processor is the only honest witness. The
    demo's headline assertion is that `rate_limit_rejections` is 0 on every PSP
    after a full flash-sale load test.

  * models realistic payment outcomes -- approvals, bank declines, and technical
    errors are three different things, not two.

  * honours idempotency keys. Replaying a key returns the stored response and
    increments a counter instead of authorising twice. In payments a retry moves
    real money, so this is not optional.

  * exposes /admin/degrade and /admin/heal so the reviewer can induce and then
    repair a processor failure mid-test without restarting anything.

Run:  uvicorn mock_psp.main:app --port 9001
Env:  PSP_ID, PSP_NAME, PSP_MAX_RPS, PSP_FEE_PERCENT, PSP_SUCCESS_RATE
"""

from __future__ import annotations

import asyncio
import os
import random
import time
from collections import deque

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

PSP_ID = os.getenv("PSP_ID", "psp-mock")
PSP_NAME = os.getenv("PSP_NAME", "Mock PSP")
MAX_RPS = int(os.getenv("PSP_MAX_RPS", "150"))
FEE_PERCENT = float(os.getenv("PSP_FEE_PERCENT", "2.5"))
BASE_SUCCESS_RATE = float(os.getenv("PSP_SUCCESS_RATE", "0.88"))

# Latency profile in seconds (healthy).
LATENCY_MIN, LATENCY_MAX = 0.005, 0.030

DECLINE_CODES = [
    ("51", "insufficient_funds"),
    ("05", "do_not_honor"),
    ("54", "expired_card"),
    ("59", "suspected_fraud"),
]

app = FastAPI(title=f"{PSP_NAME} (mock PSP)")


class AuthorizeRequest(BaseModel):
    idempotency_key: str
    amount_cents: int = Field(gt=0)
    currency: str = "MXN"
    card_bin: str | None = None
    priority: bool = False


class DegradeRequest(BaseModel):
    # "errors"  -> return 503s (the classic processor-unavailable failure)
    # "timeout" -> hang past the router's read timeout
    # "slow"    -> respond, but slowly
    # "declines"-> keep answering correctly but decline almost everything
    mode: str = "errors"
    success_rate: float = 0.30
    latency_s: float = 5.0


class _State:
    def __init__(self) -> None:
        self.window: deque[float] = deque()
        self.idempotency: dict[str, dict] = {}
        self.degraded = False
        self.degrade_mode = "errors"
        self.degrade_success_rate = 0.30
        self.degrade_latency_s = 5.0
        self.started_at = time.time()
        self.reset_counters()

    def reset_counters(self) -> None:
        self.requests_received = 0
        self.approved = 0
        self.declined = 0
        self.technical_errors = 0
        self.rate_limit_rejections = 0
        self.idempotent_replays = 0
        self.peak_rps = 0
        self.rps_samples: deque[tuple[float, int]] = deque(maxlen=600)

    def observe(self, now: float) -> int:
        """Record an arrival and return the trailing-1s arrival count."""
        cutoff = now - 1.0
        while self.window and self.window[0] <= cutoff:
            self.window.popleft()
        self.window.append(now)
        depth = len(self.window)
        if depth > self.peak_rps:
            self.peak_rps = depth
        return depth

    def current_rps(self) -> int:
        cutoff = time.monotonic() - 1.0
        while self.window and self.window[0] <= cutoff:
            self.window.popleft()
        return len(self.window)


state = _State()


@app.post("/authorize")
async def authorize(req: AuthorizeRequest, request: Request) -> JSONResponse:
    now = time.monotonic()
    state.requests_received += 1
    arrivals = state.observe(now)

    # --- rate limiting: the honest witness for Requirement 1 --------------
    if arrivals > MAX_RPS:
        state.rate_limit_rejections += 1
        return JSONResponse(
            status_code=429,
            content={
                "processor_id": PSP_ID,
                "error": "rate_limit_exceeded",
                "message": f"{PSP_NAME} accepts at most {MAX_RPS} req/s; saw {arrivals}",
                "observed_rps": arrivals,
                "max_rps": MAX_RPS,
            },
            headers={"Retry-After": "1"},
        )

    # --- idempotency ------------------------------------------------------
    cached = state.idempotency.get(req.idempotency_key)
    if cached is not None:
        state.idempotent_replays += 1
        return JSONResponse(
            status_code=200,
            content={**cached, "idempotent_replay": True},
        )

    # --- degraded behaviour ----------------------------------------------
    if state.degraded:
        if state.degrade_mode == "timeout":
            await asyncio.sleep(state.degrade_latency_s)
            state.technical_errors += 1
            return JSONResponse(
                status_code=504,
                content={"processor_id": PSP_ID, "error": "upstream_timeout"},
            )
        if state.degrade_mode == "slow":
            await asyncio.sleep(state.degrade_latency_s)
        effective_success = state.degrade_success_rate
    else:
        effective_success = BASE_SUCCESS_RATE
        await asyncio.sleep(random.uniform(LATENCY_MIN, LATENCY_MAX))

    roll = random.random()

    if state.degraded and state.degrade_mode == "errors" and roll > effective_success:
        state.technical_errors += 1
        return JSONResponse(
            status_code=503,
            content={"processor_id": PSP_ID, "error": "processor_unavailable"},
        )

    if roll > effective_success:
        # Healthy processors also fail sometimes: split the residual between a
        # bank decline (normal) and a technical error (abnormal).
        if random.random() < 0.80 or (state.degraded and state.degrade_mode == "declines"):
            code, reason = random.choice(DECLINE_CODES)
            state.declined += 1
            body = {
                "processor_id": PSP_ID,
                "status": "declined",
                "decline_code": code,
                "decline_reason": reason,
                "idempotency_key": req.idempotency_key,
                "fee_percent": FEE_PERCENT,
            }
            state.idempotency[req.idempotency_key] = body
            return JSONResponse(status_code=200, content=body)
        state.technical_errors += 1
        return JSONResponse(
            status_code=503,
            content={"processor_id": PSP_ID, "error": "processor_unavailable"},
        )

    state.approved += 1
    fee_cents = round(req.amount_cents * FEE_PERCENT / 100)
    body = {
        "processor_id": PSP_ID,
        "processor_name": PSP_NAME,
        "status": "approved",
        "authorization_code": f"{PSP_ID[-3:].upper()}{random.randint(100000, 999999)}",
        "amount_cents": req.amount_cents,
        "currency": req.currency,
        "fee_percent": FEE_PERCENT,
        "fee_cents": fee_cents,
        "idempotency_key": req.idempotency_key,
    }
    state.idempotency[req.idempotency_key] = body
    return JSONResponse(status_code=200, content=body)


@app.get("/stats")
async def stats() -> dict:
    handled = state.approved + state.declined
    return {
        "processor_id": PSP_ID,
        "processor_name": PSP_NAME,
        "max_rps": MAX_RPS,
        "fee_percent": FEE_PERCENT,
        "degraded": state.degraded,
        "degrade_mode": state.degrade_mode if state.degraded else None,
        "requests_received": state.requests_received,
        "approved": state.approved,
        "declined": state.declined,
        "technical_errors": state.technical_errors,
        "rate_limit_rejections": state.rate_limit_rejections,
        "idempotent_replays": state.idempotent_replays,
        "auth_rate": round(state.approved / handled, 4) if handled else None,
        "current_rps": state.current_rps(),
        "peak_rps_observed": state.peak_rps,
        "rate_limit_respected": state.rate_limit_rejections == 0,
        "uptime_s": round(time.time() - state.started_at, 1),
    }


@app.get("/healthz")
async def healthz() -> dict:
    return {"processor_id": PSP_ID, "ok": True, "degraded": state.degraded}


@app.post("/admin/degrade")
async def degrade(req: DegradeRequest) -> dict:
    state.degraded = True
    state.degrade_mode = req.mode
    state.degrade_success_rate = req.success_rate
    state.degrade_latency_s = req.latency_s
    return {
        "processor_id": PSP_ID,
        "degraded": True,
        "mode": req.mode,
        "success_rate": req.success_rate,
    }


@app.post("/admin/heal")
async def heal() -> dict:
    state.degraded = False
    state.degrade_mode = "errors"
    return {"processor_id": PSP_ID, "degraded": False}


@app.post("/admin/reset")
async def reset() -> dict:
    state.reset_counters()
    state.idempotency.clear()
    state.window.clear()
    return {"processor_id": PSP_ID, "reset": True}
