import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "extract"))

import extract_to_volume as ex  # noqa: E402

from carbon_lakehouse.config import Endpoint, load_config  # noqa: E402

UTC = timezone.utc
EP = Endpoint("national_intensity", "/intensity/{from}/{to}", 14, date(2024, 1, 1), 48)
NOW = datetime(2025, 6, 1, 10, 47, tzinfo=UTC)


def test_config_loads_all_endpoints():
    cfg = load_config(Path(__file__).resolve().parents[1] / "config" / "endpoints.yml")
    names = {e.name for e in cfg.enabled_endpoints()}
    assert names == {"regional_intensity", "national_intensity", "national_generation"}


def test_plan_window_first_run_uses_backfill_start():
    start, end = ex.plan_window(EP, None, NOW)
    assert start == datetime(2024, 1, 1, tzinfo=UTC)
    assert end == datetime(2025, 6, 2, 10, 30, tzinfo=UTC)  # now floored + 24h of forecasts


def test_plan_window_incremental_applies_overlap():
    start, _ = ex.plan_window(EP, "2025-06-01T09:00:00+00:00", NOW)
    assert start == datetime(2025, 6, 1, 9, tzinfo=UTC) - timedelta(hours=48)


def test_plan_window_backfill_override():
    s, e = datetime(2024, 3, 1, tzinfo=UTC), datetime(2024, 4, 1, tzinfo=UTC)
    assert ex.plan_window(EP, "2025-06-01T09:00:00+00:00", NOW, s, e) == (s, e)


def test_landing_path_is_partitioned_and_unique():
    p = ex.landing_path(EP, datetime(2025, 6, 1, tzinfo=UTC), datetime(2025, 6, 2, tzinfo=UTC), "run1", "2025-06-01")
    assert p == "national_intensity/ingest_date=2025-06-01/national_intensity_20250601T0000_20250602T0000_run1.json"


def test_local_run_end_to_end(tmp_path, monkeypatch):
    """Full run against a fake API: writes files + a watermark that only moves forward."""

    def fake_fetch(self, path_template, window):
        return {"data": [{"from": ex.to_api_ts(window.start), "to": ex.to_api_ts(window.end)}]}

    monkeypatch.setattr(ex.CarbonIntensityClient, "fetch", fake_fetch)
    args = ex.parse_args(["--mode", "local", "--volume-root", str(tmp_path),
                          "--start", "2025-05-01", "--end", "2025-05-15", "--only", "national_intensity"])
    assert ex.run(args) == 0

    files = list((tmp_path / "national_intensity").rglob("*.json"))
    assert len(files) == 1  # 14 days / chunk_days 14
    env = json.loads(files[0].read_text())
    assert env["endpoint"] == "national_intensity" and "payload" in env and env["run_id"]

    marks = json.loads((tmp_path / "_state" / "watermarks.json").read_text())
    assert marks["national_intensity"].startswith("2025-05-15")
