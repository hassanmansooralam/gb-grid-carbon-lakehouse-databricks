from datetime import datetime

from pyspark.sql import functions as F

from carbon_lakehouse import transforms as T
from carbon_lakehouse.reference import REGIONS


def test_flatten_regional_intensity(bronze):
    df = T.flatten_regional_intensity(bronze("regional_intensity"))
    assert df.count() == 4  # 2 periods x 2 regions
    row = df.where("region_id = 1 AND period_start_utc = '2025-06-01 00:00:00'").first()
    assert row.forecast_gco2_kwh == 10
    assert row.region_name == "North Scotland"
    assert row.period_end_utc == datetime(2025, 6, 1, 0, 30)
    assert row.extracted_at is not None and row._source_file == "regional_intensity.json"


def test_flatten_regional_generation_is_long_and_sums_to_100(bronze):
    df = T.flatten_regional_generation(bronze("regional_intensity"))
    assert df.count() == 4 * 9  # 9 fuels in the fixture
    totals = df.groupBy("region_id", "period_start_utc").agg(F.sum("perc").alias("t")).collect()
    assert all(abs(r.t - 100) < 0.01 for r in totals)


def test_flatten_national_keeps_null_actuals(bronze):
    df = T.flatten_national_intensity(bronze("national_intensity"))
    actuals = {r.period_start_utc.minute: r.actual_gco2_kwh for r in df.collect()}
    assert actuals == {0: 110, 30: None}


def test_pivot_fuel_mix_groups(bronze):
    mix = T.flatten_regional_generation(bronze("regional_intensity"))
    wide = T.pivot_fuel_mix(mix, ["period_start_utc", "region_id"])
    r = wide.where("region_id = 1 AND period_start_utc = '2025-06-01 00:00:00'").first()
    assert r.wind_pct == 60.0
    assert r.renewable_pct == round(60.0 + 8.0 + 2.0, 1)       # wind + solar + hydro
    assert r.low_carbon_pct == round(r.renewable_pct + 15.0, 1)  # + nuclear
    assert r.oil_pct is None  # missing fuel -> null column, schema stays fixed


def test_uk_time_handles_bst(spark):
    # 1 June is BST (UTC+1): 00:00 UTC is 01:00 UK time -> slot 2
    df = spark.createDataFrame([(datetime(2025, 6, 1, 0, 0),)], "period_start_utc timestamp")
    r = T.add_uk_time_columns(df).first()
    assert r.hour_uk == 1 and r.half_hour_slot == 2 and r.day_of_week == "Sun"


def test_regional_fact_and_daily_summary(spark, bronze):
    raw = bronze("regional_intensity")
    intensity = T.flatten_regional_intensity(raw).where("forecast_gco2_kwh BETWEEN 0 AND 1000")
    wide = T.pivot_fuel_mix(T.flatten_regional_generation(raw), ["period_start_utc", "region_id"])
    dim = spark.createDataFrame(REGIONS, "region_id int, region_name string, country string, is_aggregate boolean")

    fact = T.build_regional_fact(intensity, wide, dim)
    assert fact.count() == 3  # the 9999 row was filtered out
    assert {"renewable_pct", "country", "half_hour_slot", "date_uk"} <= set(fact.columns)

    daily = T.build_region_daily_summary(fact)
    scot = daily.where("region_id = 1").first()
    assert scot.avg_gco2_kwh == 11.0 and scot.periods == 2 and scot.low_carbon_periods == 2


def test_greenest_slots_ranks(spark):
    rows = [(1, "North Scotland", "Mon", 2, s, float(v), datetime.now().date()) for s, v in [(0, 50), (1, 20), (2, 80)]]
    fact = spark.createDataFrame(
        rows, "region_id int, region_name string, day_of_week string, day_of_week_num int, "
              "half_hour_slot int, forecast_gco2_kwh double, date_uk date")
    out = {r.half_hour_slot: (r.rank_in_day, r.slot_label) for r in T.build_greenest_slots(fact).collect()}
    assert out[1] == (1, "00:30") and out[2][0] == 3


def test_forecast_accuracy(bronze):
    acc = T.build_forecast_accuracy(T.flatten_national_intensity(bronze("national_intensity"))).first()
    assert acc.periods == 1          # null actual excluded
    assert acc.mae_gco2_kwh == 10.0  # |120 - 110|
    assert acc.bias_gco2_kwh == 10.0
