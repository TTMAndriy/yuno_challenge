"""Configuration loading. Single JSON file, env-overridable base URLs."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "processors.json"


@dataclass(frozen=True)
class ProcessorConfig:
    id: str
    name: str
    base_url: str
    max_rps: int
    fee_percent: float
    baseline: bool = False


@dataclass(frozen=True)
class HealthConfig:
    window_seconds: int = 60
    min_samples: int = 20
    technical_success_threshold: float = 0.60
    auth_rate_floor: float = 0.30
    open_cooldown_seconds: int = 30
    probe_requests: int = 2


@dataclass(frozen=True)
class AdmissionConfig:
    queue_timeout_ms: int = 2500
    priority_queue_timeout_ms: int = 5000
    max_queue_depth: int = 2000
    max_priority_queue_depth: int = 500
    poll_interval_ms: int = 10


@dataclass(frozen=True)
class UpstreamConfig:
    connect_timeout_s: float = 1.0
    read_timeout_s: float = 2.0
    max_attempts: int = 2
    rate_limit_headroom: float = 0.95
    inflight_ratio: float = 0.30


@dataclass(frozen=True)
class AppConfig:
    routing_strategy: str = "cost_aware"
    health: HealthConfig = field(default_factory=HealthConfig)
    admission: AdmissionConfig = field(default_factory=AdmissionConfig)
    upstream: UpstreamConfig = field(default_factory=UpstreamConfig)
    processors: tuple[ProcessorConfig, ...] = ()

    @property
    def total_capacity_rps(self) -> int:
        return sum(p.max_rps for p in self.processors)

    def baseline_processor(self) -> ProcessorConfig:
        for p in self.processors:
            if p.baseline:
                return p
        # Fall back to the most expensive, which is the worst-case baseline.
        return max(self.processors, key=lambda p: p.fee_percent)


def load_config(path: str | Path | None = None) -> AppConfig:
    path = Path(path or os.getenv("ROUTER_CONFIG", DEFAULT_CONFIG_PATH))
    raw = json.loads(path.read_text())

    processors = []
    for entry in raw["processors"]:
        base_url = os.getenv(f"PSP_URL_{entry['id'].replace('-', '_').upper()}", entry["base_url"])
        processors.append(
            ProcessorConfig(
                id=entry["id"],
                name=entry["name"],
                base_url=base_url.rstrip("/"),
                max_rps=int(entry["max_rps"]),
                fee_percent=float(entry["fee_percent"]),
                baseline=bool(entry.get("baseline", False)),
            )
        )
    if len(processors) < 3:
        raise ValueError("at least 3 processors are required")

    strategy = os.getenv("ROUTING_STRATEGY", raw.get("routing_strategy", "cost_aware"))
    if strategy not in {"cost_aware", "balanced"}:
        raise ValueError(f"unknown routing_strategy: {strategy}")

    return AppConfig(
        routing_strategy=strategy,
        health=HealthConfig(**raw.get("health", {})),
        admission=AdmissionConfig(**raw.get("admission", {})),
        upstream=UpstreamConfig(**raw.get("upstream", {})),
        processors=tuple(processors),
    )
