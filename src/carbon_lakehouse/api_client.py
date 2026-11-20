"""Client for the NESO Carbon Intensity API (https://api.carbonintensity.org.uk).

Data is licensed under CC BY 4.0.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import requests

API_TS_FORMAT = "%Y-%m-%dT%H:%MZ"


def to_api_ts(dt: datetime) -> str:
    """Format a UTC datetime the way the API expects."""
    return dt.astimezone(timezone.utc).strftime(API_TS_FORMAT)


def floor_to_half_hour(dt: datetime) -> datetime:
    """Round down to the nearest half hour."""
    return dt.replace(minute=0 if dt.minute < 30 else 30, second=0, microsecond=0)


@dataclass(frozen=True)
class Window:
    start: datetime
    end: datetime


def build_windows(start: datetime, end: datetime, chunk_days: int) -> list[Window]:
    """Split [start, end) into consecutive windows of at most `chunk_days`."""
    if chunk_days <= 0:
        raise ValueError("chunk_days must be positive")
    if end <= start:
        return []

    windows: list[Window] = []
    cursor = start
    step = timedelta(days=chunk_days)
    while cursor < end:
        nxt = min(cursor + step, end)
        windows.append(Window(cursor, nxt))
        cursor = nxt
    return windows


class CarbonIntensityClient:
    """HTTP client with retries and exponential backoff."""

    def __init__(
        self,
        base_url: str,
        session: requests.Session | None = None,
        max_retries: int = 4,
        backoff_seconds: float = 2.0,
        timeout_seconds: int = 60,
    ):
        self.base_url = base_url.rstrip("/")
        self.session = session or requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds
        self.timeout_seconds = timeout_seconds

    def build_url(self, path_template: str, window: Window) -> str:
        path = path_template.format(**{"from": to_api_ts(window.start), "to": to_api_ts(window.end)})
        return f"{self.base_url}{path}"

    def fetch(self, path_template: str, window: Window) -> dict:
        """GET one window. Retries on 429 / 5xx / network errors; fails fast on other 4xx."""
        url = self.build_url(path_template, window)
        last_error: Exception | None = None

        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.get(url, timeout=self.timeout_seconds)
                if resp.status_code == 429 or resp.status_code >= 500:
                    raise requests.HTTPError(f"retryable status {resp.status_code}", response=resp)
                resp.raise_for_status()
                body = resp.json()
                if "data" not in body:
                    raise ValueError(f"Unexpected response shape from {url}: keys={list(body)}")
                return body
            except requests.HTTPError as e:
                status = e.response.status_code if e.response is not None else None
                if status is not None and 400 <= status < 500 and status != 429:
                    raise
                last_error = e
            except (requests.ConnectionError, requests.Timeout) as e:
                last_error = e

            if attempt < self.max_retries:
                time.sleep(self.backoff_seconds * (2 ** (attempt - 1)))

        raise RuntimeError(f"Failed after {self.max_retries} attempts: {url}") from last_error
