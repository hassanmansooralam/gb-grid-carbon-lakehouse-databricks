import numpy as np
import pandas as pd

from carbon_lakehouse.features import FEATURE_COLUMNS, build_features, extend_for_scoring, time_split


def _series(days=10):
    idx = pd.date_range("2025-03-01", periods=48 * days, freq="30min", tz="UTC")
    return pd.DataFrame({"period_start_utc": idx, "actual_gco2_kwh": np.arange(len(idx), dtype=float)})


def test_lags_never_look_less_than_24h_back():
    f = build_features(_series())
    row = f.iloc[100]
    assert row["lag_48"] == 100 - 48
    assert np.isnan(f.iloc[47]["lag_48"])  # not enough history yet
    assert set(FEATURE_COLUMNS) <= set(f.columns)


def test_missing_periods_are_reindexed_so_lags_stay_aligned():
    df = _series().drop(index=[60, 61])
    f = build_features(df)
    assert len(f) == 48 * 10
    assert f.iloc[110]["lag_48"] == 62  # row 62 still sits 48 slots before row 110


def test_extend_for_scoring_adds_future_rows_with_features():
    hist = _series()
    ext = build_features(extend_for_scoring(hist, horizon=48))
    future = ext.tail(48)
    assert future["actual_gco2_kwh"].isna().all()
    assert future["lag_48"].notna().all()  # all computable from history -> no leakage


def test_time_split_is_chronological():
    train, test = time_split(build_features(_series()), test_days=2)
    assert train["period_start_utc"].max() < test["period_start_utc"].min()
