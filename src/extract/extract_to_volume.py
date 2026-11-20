"""Extract Carbon Intensity API data and land raw JSON in a Unity Catalog Volume.

Runs outside Databricks (GitHub Actions or locally) and uploads with the Databricks
SDK, because Databricks Free Edition restricts outbound internet access. On a
workspace with internet access it can run as a job task with --mode local.

Per endpoint in config/endpoints.yml:
  1. Read the watermark from _state/watermarks.json
  2. Pull (watermark - overlap) -> now + 24h, split into chunks
  3. Write each response, wrapped with lineage metadata, to
     <endpoint>/ingest_date=YYYY-MM-DD/<endpoint>_<from>_<to>_<run_id>.json
  4. Advance the watermark (never backwards)

Usage:
  python src/extract/extract_to_volume.py --mode sdk --volume-root /Volumes/carbon_dev/raw/landing
  python src/extract/extract_to_volume.py --mode sdk --volume-root ... --start 2024-01-01 --end 2024-03-01
  python src/extract/extract_to_volume.py --mode local --volume-root ./_local_landing

--mode sdk authenticates via standard Databricks env vars or a config profile.
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from carbon_lakehouse.api_client import (  # noqa: E402
    CarbonIntensityClient,
    build_windows,
    floor_to_half_hour,
    to_api_ts,
)
from carbon_lakehouse.config import Endpoint, load_config  # noqa: E402

log = logging.getLogger("extract")

STATE_FILE = "_state/watermarks.json"
FORECAST_AHEAD_HOURS = 24


class LocalStorage:
    """Local filesystem; also works for /Volumes paths inside Databricks."""

    def __init__(self, root: str):
        self.root = Path(root)

    def read_text(self, rel_path: str) -> str | None:
        p = self.root / rel_path
        return p.read_text(encoding="utf-8") if p.exists() else None

    def write_text(self, rel_path: str, text: str) -> None:
        p = self.root / rel_path
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text, encoding="utf-8")

    def describe(self) -> str:
        return f"local:{self.root}"


class SdkStorage:
    """Unity Catalog Volume via the Databricks Files API."""

    def __init__(self, root: str):
        from databricks.sdk import WorkspaceClient

        self.root = root.rstrip("/")
        self.w = WorkspaceClient()

    def read_text(self, rel_path: str) -> str | None:
        from databricks.sdk.errors import NotFound

        try:
            resp = self.w.files.download(f"{self.root}/{rel_path}")
            return resp.contents.read().decode("utf-8")
        except NotFound:
            return None

    def write_text(self, rel_path: str, text: str) -> None:
        self.w.files.upload(f"{self.root}/{rel_path}", io.BytesIO(text.encode("utf-8")), overwrite=True)

    def describe(self) -> str:
        return f"uc-volume:{self.root}"


def load_watermarks(storage) -> dict[str, str]:
    raw = storage.read_text(STATE_FILE)
    return json.loads(raw) if raw else {}


def save_watermarks(storage, marks: dict[str, str]) -> None:
    storage.write_text(STATE_FILE, json.dumps(marks, indent=2, sort_keys=True))


def plan_window(
    ep: Endpoint,
    watermark: str | None,
    now: datetime,
    start_override: datetime | None = None,
    end_override: datetime | None = None,
) -> tuple[datetime, datetime]:
    """Return the (start, end) range to pull for an endpoint."""
    if start_override:
        start = start_override
    elif watermark:
        start = datetime.fromisoformat(watermark) - timedelta(hours=ep.overlap_hours)
    else:
        start = datetime(ep.backfill_start.year, ep.backfill_start.month, ep.backfill_start.day, tzinfo=timezone.utc)

    end = end_override or (floor_to_half_hour(now) + timedelta(hours=FORECAST_AHEAD_HOURS))
    return start, end


def make_envelope(ep: Endpoint, start: datetime, end: datetime, run_id: str, payload: dict) -> dict:
    """Wrap the raw API response with lineage metadata."""
    return {
        "endpoint": ep.name,
        "request_from": to_api_ts(start),
        "request_to": to_api_ts(end),
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id,
        "payload": payload,
    }


def landing_path(ep: Endpoint, start: datetime, end: datetime, run_id: str, ingest_date: str) -> str:
    f = start.strftime("%Y%m%dT%H%M")
    t = end.strftime("%Y%m%dT%H%M")
    return f"{ep.name}/ingest_date={ingest_date}/{ep.name}_{f}_{t}_{run_id}.json"


def run(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    storage = SdkStorage(args.volume_root) if args.mode == "sdk" else LocalStorage(args.volume_root)
    client = CarbonIntensityClient(cfg.base_url)

    now = datetime.now(timezone.utc)
    run_id = now.strftime("%Y%m%d%H%M%S") + "-" + uuid.uuid4().hex[:6]
    ingest_date = now.strftime("%Y-%m-%d")
    marks = load_watermarks(storage)

    start_override = _parse_date(args.start)
    end_override = _parse_date(args.end)

    log.info("run_id=%s target=%s", run_id, storage.describe())
    files_written = 0

    for ep in cfg.enabled_endpoints():
        if args.only and ep.name not in args.only:
            continue
        start, end = plan_window(ep, marks.get(ep.name), now, start_override, end_override)
        windows = build_windows(start, end, ep.chunk_days)
        log.info("%s: %s -> %s in %d chunk(s)", ep.name, to_api_ts(start), to_api_ts(end), len(windows))

        for w in windows:
            payload = client.fetch(ep.path_template, w)
            env = make_envelope(ep, w.start, w.end, run_id, payload)
            storage.write_text(landing_path(ep, w.start, w.end, run_id, ingest_date), json.dumps(env))
            files_written += 1

        # Watermark tracks "now" (end includes future forecasts) and never moves backwards.
        new_mark = floor_to_half_hour(min(end, now)).isoformat()
        if ep.name not in marks or new_mark > marks[ep.name]:
            marks[ep.name] = new_mark

    save_watermarks(storage, marks)
    log.info("done: %d file(s) written", files_written)
    return 0


def _parse_date(s: str | None) -> datetime | None:
    if not s:
        return None
    d = datetime.fromisoformat(s)
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[2]
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--mode", choices=["sdk", "local"], default="sdk")
    landing_env = os.environ.get("LANDING_ROOT")
    p.add_argument("--volume-root", default=landing_env, required=landing_env is None)
    p.add_argument("--config", default=str(repo_root / "config" / "endpoints.yml"))
    p.add_argument("--start", help="Backfill start, e.g. 2024-01-01")
    p.add_argument("--end", help="Backfill end, e.g. 2024-03-01")
    p.add_argument("--only", nargs="*", help="Only these endpoint names")
    return p.parse_args(argv)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    sys.exit(run(parse_args()))
