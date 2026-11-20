from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
import requests

from carbon_lakehouse.api_client import (
    CarbonIntensityClient,
    Window,
    build_windows,
    floor_to_half_hour,
    to_api_ts,
)

UTC = timezone.utc


def test_to_api_ts_format():
    assert to_api_ts(datetime(2025, 1, 2, 3, 30, tzinfo=UTC)) == "2025-01-02T03:30Z"


@pytest.mark.parametrize("minute,expected", [(0, 0), (29, 0), (30, 30), (59, 30)])
def test_floor_to_half_hour(minute, expected):
    assert floor_to_half_hour(datetime(2025, 1, 1, 10, minute, 45, tzinfo=UTC)).minute == expected


def test_build_windows_splits_and_covers_range():
    start, end = datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 1, 20, tzinfo=UTC)
    windows = build_windows(start, end, chunk_days=7)
    assert [w.start.day for w in windows] == [1, 8, 15]
    assert windows[-1].end == end
    # no gaps or overlaps
    assert all(a.end == b.start for a, b in zip(windows, windows[1:], strict=False))


def test_build_windows_empty_and_invalid():
    t = datetime(2025, 1, 1, tzinfo=UTC)
    assert build_windows(t, t, 7) == []
    with pytest.raises(ValueError):
        build_windows(t, t, 0)


def _resp(status, body=None):
    r = MagicMock()
    r.status_code = status
    r.json.return_value = body or {}
    if status >= 400:
        r.raise_for_status.side_effect = requests.HTTPError(response=r)
    return r


def _client(session):
    return CarbonIntensityClient("https://api.example", session=session, backoff_seconds=0)


def test_build_url():
    c = _client(MagicMock())
    w = Window(datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 1, 2, tzinfo=UTC))
    assert c.build_url("/intensity/{from}/{to}", w) == "https://api.example/intensity/2025-01-01T00:00Z/2025-01-02T00:00Z"


def test_fetch_retries_then_succeeds():
    session = MagicMock()
    session.get.side_effect = [_resp(503), _resp(429), _resp(200, {"data": [1]})]
    w = Window(datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 1, 2, tzinfo=UTC))
    assert _client(session).fetch("/x/{from}/{to}", w) == {"data": [1]}
    assert session.get.call_count == 3


def test_fetch_does_not_retry_client_errors():
    session = MagicMock()
    session.get.return_value = _resp(400)
    w = Window(datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 1, 2, tzinfo=UTC))
    with pytest.raises(requests.HTTPError):
        _client(session).fetch("/x/{from}/{to}", w)
    assert session.get.call_count == 1


def test_fetch_gives_up_after_max_retries():
    session = MagicMock()
    session.get.return_value = _resp(500)
    w = Window(datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 1, 2, tzinfo=UTC))
    with pytest.raises(RuntimeError):
        _client(session).fetch("/x/{from}/{to}", w)
    assert session.get.call_count == 4
