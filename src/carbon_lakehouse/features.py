"""Feature engineering for the 24h-ahead national carbon intensity forecast.

All lag features are at least HORIZON (48 half-hours) old, so every feature is
available at prediction time for the full 24-hour horizon.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

HORIZON = 48  # half-hours

FEATURE_COLUMNS = [
    "half_hour_slot",
    "day_of_week",
    "month",
    "is_weekend",
    "slot_sin",
    "slot_cos",
    "lag_48",
    "lag_96",
    "lag_336",
    "roll_mean_48_lag48",
]
TARGET = "actual_gco2_kwh"


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """Build features from columns period_start_utc and actual_gco2_kwh.

    The series is re-indexed onto a complete 30-minute grid so lags stay aligned
    when periods are missing.
    """
    out = df[["period_start_utc", TARGET]].copy()
    out["period_start_utc"] = pd.to_datetime(out["period_start_utc"], utc=True)
    out = out.drop_duplicates("period_start_utc").set_index("period_start_utc").sort_index()

    full = pd.date_range(out.index.min(), out.index.max(), freq="30min", tz="UTC")
    out = out.reindex(full)
    out.index.name = "period_start_utc"

    local = out.index.tz_convert("Europe/London")
    out["half_hour_slot"] = local.hour * 2 + (local.minute >= 30).astype(int)
    out["day_of_week"] = local.dayofweek
    out["month"] = local.month
    out["is_weekend"] = (out["day_of_week"] >= 5).astype(int)
    out["slot_sin"] = np.sin(2 * np.pi * out["half_hour_slot"] / 48)
    out["slot_cos"] = np.cos(2 * np.pi * out["half_hour_slot"] / 48)

    y = out[TARGET]
    out["lag_48"] = y.shift(48)
    out["lag_96"] = y.shift(96)
    out["lag_336"] = y.shift(336)
    out["roll_mean_48_lag48"] = y.shift(HORIZON).rolling(48, min_periods=24).mean()

    return out.reset_index()


def extend_for_scoring(history: pd.DataFrame, horizon: int = HORIZON) -> pd.DataFrame:
    """Append `horizon` future rows with an unknown target."""
    history = history[["period_start_utc", TARGET]].copy()
    history["period_start_utc"] = pd.to_datetime(history["period_start_utc"], utc=True)
    last = history["period_start_utc"].max()
    future = pd.DataFrame(
        {"period_start_utc": pd.date_range(last + pd.Timedelta(minutes=30), periods=horizon, freq="30min", tz="UTC")}
    )
    future[TARGET] = np.nan
    return pd.concat([history, future], ignore_index=True)


def time_split(df: pd.DataFrame, test_days: int = 30) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Chronological split: the last `test_days` form the test set."""
    cutoff = df["period_start_utc"].max() - pd.Timedelta(days=test_days)
    return df[df["period_start_utc"] <= cutoff], df[df["period_start_utc"] > cutoff]
