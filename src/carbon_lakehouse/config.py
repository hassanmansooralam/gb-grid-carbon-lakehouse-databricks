"""Load the endpoint configuration (config/endpoints.yml)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import yaml


@dataclass(frozen=True)
class Endpoint:
    name: str
    path_template: str
    chunk_days: int
    backfill_start: date
    overlap_hours: int
    enabled: bool = True
    description: str = ""


@dataclass(frozen=True)
class IngestConfig:
    base_url: str
    endpoints: list[Endpoint]

    def enabled_endpoints(self) -> list[Endpoint]:
        return [e for e in self.endpoints if e.enabled]


def load_config(path: str | Path) -> IngestConfig:
    with open(path, encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    endpoints = [
        Endpoint(
            name=e["name"],
            path_template=e["path_template"],
            chunk_days=int(e["chunk_days"]),
            backfill_start=date.fromisoformat(str(e["backfill_start"])),
            overlap_hours=int(e.get("overlap_hours", 0)),
            enabled=bool(e.get("enabled", True)),
            description=e.get("description", ""),
        )
        for e in raw["endpoints"]
    ]
    return IngestConfig(base_url=raw["base_url"].rstrip("/"), endpoints=endpoints)
