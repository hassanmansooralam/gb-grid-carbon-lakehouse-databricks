"""Gold: materialized views for dashboards, Genie and ML."""

from pyspark import pipelines as dp

from carbon_lakehouse import transforms as T

GOLD = {"quality": "gold"}


@dp.temporary_view(name="v_regional_mix_wide")
def v_regional_mix_wide():
    mix = spark.read.table("silver.regional_generation_mix")  # noqa: F821
    return T.pivot_fuel_mix(mix, keys=["period_start_utc", "region_id"])


@dp.materialized_view(
    name="gold.fact_regional_halfhour",
    comment="One row per region x half-hour: forecast intensity, fuel mix %, renewable/low-carbon share, UK time.",
    cluster_by=["date_uk", "region_id"],
    table_properties=GOLD,
)
def fact_regional_halfhour():
    return T.build_regional_fact(
        spark.read.table("silver.regional_intensity"),  # noqa: F821
        spark.read.table("v_regional_mix_wide"),  # noqa: F821
        spark.read.table("silver.dim_region"),  # noqa: F821
    )


@dp.materialized_view(
    name="gold.region_daily_summary",
    comment="Daily average/min/max intensity and renewable share per region (UK calendar day).",
    table_properties=GOLD,
)
def region_daily_summary():
    return T.build_region_daily_summary(spark.read.table("gold.fact_regional_halfhour"))  # noqa: F821


@dp.materialized_view(
    name="gold.greenest_slots",
    comment="Typical intensity by region x weekday x half-hour slot (last 90 days). rank_in_day=1 is greenest.",
    table_properties=GOLD,
)
def greenest_slots():
    return T.build_greenest_slots(spark.read.table("gold.fact_regional_halfhour"))  # noqa: F821


@dp.materialized_view(
    name="gold.national_halfhour",
    comment="National forecast vs actual per half-hour with national fuel mix. Base table for ML.",
    cluster_by=["period_start_utc"],
    table_properties=GOLD,
)
def national_halfhour():
    nat = spark.read.table("silver.national_intensity")  # noqa: F821
    mix = T.pivot_fuel_mix(spark.read.table("silver.national_generation_mix"), keys=["period_start_utc"])  # noqa: F821
    joined = nat.drop("extracted_at", "run_id", "_source_file").join(mix, "period_start_utc", "left")
    return T.add_uk_time_columns(joined)


@dp.materialized_view(
    name="gold.national_forecast_accuracy",
    comment="Daily MAE / MAPE / bias of the official NESO national forecast vs actual.",
    table_properties=GOLD,
)
def national_forecast_accuracy():
    return T.build_forecast_accuracy(spark.read.table("silver.national_intensity"))  # noqa: F821
