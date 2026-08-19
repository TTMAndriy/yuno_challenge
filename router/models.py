"""Public API contract."""

from __future__ import annotations

from pydantic import BaseModel, Field


class AuthorizationRequest(BaseModel):
    amount_cents: int = Field(gt=0, description="Amount in minor units")
    currency: str = Field(default="MXN", min_length=3, max_length=3)
    order_id: str | None = None
    card_bin: str | None = Field(default=None, max_length=8)
    idempotency_key: str | None = Field(
        default=None,
        description="Caller-supplied key. Generated if absent. Preserved across "
                    "failover so a retry on a second processor can never "
                    "double-authorise.",
    )
    priority: bool = Field(
        default=False,
        description="High-value transaction. Gets a larger admission allowance, "
                    "a longer queue deadline, and the most reliable processor "
                    "regardless of cost.",
    )


class AttemptRecord(BaseModel):
    processor_id: str
    outcome: str
    http_status: int | None = None
    latency_ms: float
    retryable: bool


class AuthorizationResponse(BaseModel):
    status: str  # approved | declined | failed
    request_id: str
    idempotency_key: str
    processor_id: str | None = None
    processor_name: str | None = None
    authorization_code: str | None = None
    decline_code: str | None = None
    decline_reason: str | None = None
    error: str | None = None
    message: str | None = None
    fee_percent: float | None = None
    fee_cents: int | None = None
    queued_ms: float = 0.0
    total_latency_ms: float = 0.0
    attempts: list[AttemptRecord] = Field(default_factory=list)
