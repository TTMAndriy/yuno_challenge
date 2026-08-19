"""HTTP client for the processors, plus outcome classification.

Classification is where the retry policy lives, and getting it wrong in payments
costs real money:

  APPROVED         terminal. Done.
  DECLINED         terminal and NOT retryable. The bank said no. Retrying the
                   same card on a second processor will almost always be
                   declined again, adds load during an incident, and in the worst
                   case looks like card testing to a fraud system.
  TECHNICAL_ERROR  retryable. Timeout, connection failure, 5xx, or 429 -- the
                   processor never gave us the bank's answer, so we do not know
                   the outcome and may safely try elsewhere with the SAME
                   idempotency key.

A 429 from a processor also means our own limiter mis-sized itself, so it is
counted separately and logged loudly. In a correct run that counter stays at 0.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

import httpx

from .health import Outcome


@dataclass
class UpstreamResult:
    outcome: Outcome
    retryable: bool
    http_status: int | None
    latency_ms: float
    body: dict
    rate_limited: bool = False


class UpstreamClient:
    def __init__(self, connect_timeout_s: float, read_timeout_s: float) -> None:
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(
                connect=connect_timeout_s,
                read=read_timeout_s,
                write=read_timeout_s,
                pool=connect_timeout_s,
            ),
            limits=httpx.Limits(max_connections=2000, max_keepalive_connections=1000),
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def authorize(
        self,
        base_url: str,
        idempotency_key: str,
        amount_cents: int,
        currency: str,
        card_bin: str | None,
        priority: bool,
    ) -> UpstreamResult:
        payload = {
            "idempotency_key": idempotency_key,
            "amount_cents": amount_cents,
            "currency": currency,
            "card_bin": card_bin,
            "priority": priority,
        }
        started = time.perf_counter()
        try:
            resp = await self._client.post(f"{base_url}/authorize", json=payload)
        except httpx.TimeoutException:
            return UpstreamResult(
                outcome=Outcome.TECHNICAL_ERROR,
                retryable=True,
                http_status=None,
                latency_ms=(time.perf_counter() - started) * 1000,
                body={"error": "upstream_timeout"},
            )
        except httpx.HTTPError as exc:
            return UpstreamResult(
                outcome=Outcome.TECHNICAL_ERROR,
                retryable=True,
                http_status=None,
                latency_ms=(time.perf_counter() - started) * 1000,
                body={"error": "upstream_unreachable", "detail": str(exc)[:200]},
            )

        latency_ms = (time.perf_counter() - started) * 1000
        try:
            body = resp.json()
        except ValueError:
            body = {"error": "invalid_upstream_response"}

        if resp.status_code == 429:
            return UpstreamResult(
                outcome=Outcome.TECHNICAL_ERROR,
                retryable=True,
                http_status=429,
                latency_ms=latency_ms,
                body=body,
                rate_limited=True,
            )

        if resp.status_code >= 500:
            return UpstreamResult(
                outcome=Outcome.TECHNICAL_ERROR,
                retryable=True,
                http_status=resp.status_code,
                latency_ms=latency_ms,
                body=body,
            )

        if resp.status_code >= 400:
            # Malformed request on our side. Retrying will not help.
            return UpstreamResult(
                outcome=Outcome.TECHNICAL_ERROR,
                retryable=False,
                http_status=resp.status_code,
                latency_ms=latency_ms,
                body=body,
            )

        status = str(body.get("status", "")).lower()
        if status == "approved":
            return UpstreamResult(Outcome.APPROVED, False, resp.status_code, latency_ms, body)
        if status == "declined":
            return UpstreamResult(Outcome.DECLINED, False, resp.status_code, latency_ms, body)

        return UpstreamResult(
            outcome=Outcome.TECHNICAL_ERROR,
            retryable=True,
            http_status=resp.status_code,
            latency_ms=latency_ms,
            body=body or {"error": "unrecognised_upstream_status"},
        )
