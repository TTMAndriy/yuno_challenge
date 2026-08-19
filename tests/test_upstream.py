"""Outcome classification -- this is where the retry policy actually lives.

Getting these wrong costs real money: a retried decline can look like card
testing to a fraud system, and a non-retried timeout abandons a checkout whose
outcome we never learned.
"""

import httpx
import pytest

from router.health import Outcome
from router.upstream import UpstreamClient


def classify(status_code: int, body: dict):
    """Exercise the classifier through a mock transport, not by reimplementing it."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code, json=body)

    client = UpstreamClient(1.0, 2.0)
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return client


@pytest.mark.asyncio
async def test_approved_is_terminal():
    c = classify(200, {"status": "approved", "authorization_code": "X1"})
    r = await c.authorize("http://psp", "k", 100, "MXN", None, False)
    assert r.outcome is Outcome.APPROVED
    assert r.retryable is False


@pytest.mark.asyncio
async def test_decline_is_not_retryable():
    """The bank answered. Retrying the same card elsewhere is not a fix."""
    c = classify(200, {"status": "declined", "decline_code": "51"})
    r = await c.authorize("http://psp", "k", 100, "MXN", None, False)
    assert r.outcome is Outcome.DECLINED
    assert r.retryable is False


@pytest.mark.asyncio
@pytest.mark.parametrize("code", [500, 502, 503, 504])
async def test_server_errors_are_retryable(code):
    c = classify(code, {"error": "processor_unavailable"})
    r = await c.authorize("http://psp", "k", 100, "MXN", None, False)
    assert r.outcome is Outcome.TECHNICAL_ERROR
    assert r.retryable is True


@pytest.mark.asyncio
async def test_429_is_retryable_and_flagged_as_our_bug():
    """A 429 means our own limiter over-admitted. It must be visible, not silent."""
    c = classify(429, {"error": "rate_limit_exceeded", "observed_rps": 151, "max_rps": 150})
    r = await c.authorize("http://psp", "k", 100, "MXN", None, False)
    assert r.outcome is Outcome.TECHNICAL_ERROR
    assert r.retryable is True
    assert r.rate_limited is True


@pytest.mark.asyncio
async def test_client_error_is_not_retryable():
    """A malformed request is our bug. Retrying it just doubles the load."""
    c = classify(400, {"error": "bad_request"})
    r = await c.authorize("http://psp", "k", 100, "MXN", None, False)
    assert r.outcome is Outcome.TECHNICAL_ERROR
    assert r.retryable is False


@pytest.mark.asyncio
async def test_timeout_is_retryable():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("timed out", request=request)

    c = UpstreamClient(1.0, 2.0)
    c._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    r = await c.authorize("http://psp", "k", 100, "MXN", None, False)
    assert r.outcome is Outcome.TECHNICAL_ERROR
    assert r.retryable is True
    assert r.body["error"] == "upstream_timeout"


@pytest.mark.asyncio
async def test_unrecognised_status_is_treated_as_a_failure_not_a_success():
    c = classify(200, {"status": "banana"})
    r = await c.authorize("http://psp", "k", 100, "MXN", None, False)
    assert r.outcome is Outcome.TECHNICAL_ERROR


@pytest.mark.asyncio
async def test_idempotency_key_is_forwarded_to_the_processor():
    """A retry across processors must carry the same key or it can double-charge."""
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        import json

        seen.update(json.loads(request.content))
        return httpx.Response(200, json={"status": "approved"})

    c = UpstreamClient(1.0, 2.0)
    c._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    await c.authorize("http://psp", "order-abc", 4200, "MXN", "411111", True)
    assert seen["idempotency_key"] == "order-abc"
    assert seen["amount_cents"] == 4200
    assert seen["priority"] is True
