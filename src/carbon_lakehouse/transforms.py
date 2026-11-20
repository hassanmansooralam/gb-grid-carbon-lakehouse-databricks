"""PySpark transformations for the silver and gold layers.

Pure DataFrame-in, DataFrame-out functions with no I/O, so they can be unit-tested
locally. Pipeline definitions in src/pipelines/ handle reads and table creation.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from carbon_lakehouse.reference import FOSSIL_FUELS, FUELS, LOW_CARBON_FUELS, RENEWABLE_FUELS

UK_TZ = "Europe/London"
API_TS = "yyyy-MM-dd'T'HH:mmX"

# Explicit bronze schemas used by Auto Loader.
_MIX = "array<struct<fuel:string,perc:double>>"
_ENVELOPE = "endpoint string, request_from string, request_to string, extracted_at string, run_id string"

BRONZE_SCHEMAS = {
    "regional_intensity": (
        f"{_ENVELOPE}, payload struct<data:array<struct<from:string,to:string,"
        f"regions:array<struct<regionid:int,dnoregion:string,shortname:string,"
        f"intensity:struct<forecast:int,index:string>,generationmix:{_MIX}>>>>>"
    ),
    "national_intensity": (
        f"{_ENVELOPE}, payload struct<data:array<struct<from:string,to:string,"
        f"intensity:struct<forecast:int,actual:int,index:string>>>>"
    ),
    "national_generation": (
        f"{_ENVELOPE}, payload struct<data:array<struct<from:string,to:string,generationmix:{_MIX}>>>"
    ),
}


def _lineage():
    """Lineage columns carried from bronze into silver."""
    return [
        F.col("extracted_at").cast("timestamp").alias("extracted_at"),
        F.col("run_id"),
        F.col("_source_file"),
    ]


# --- Silver -------------------------------------------------------------------
def flatten_regional_intensity(bronze: DataFrame) -> DataFrame:
    """Flatten payload.data[].regions[] to one row per (region, half-hour)."""
    periods = bronze.select(*_lineage(), F.explode("payload.data").alias("p"))
    regions = periods.select("*", F.explode("p.regions").alias("r"))
    return regions.select(
        F.to_timestamp(F.col("p.from"), API_TS).alias("period_start_utc"),
        F.to_timestamp(F.col("p.to"), API_TS).alias("period_end_utc"),
        F.col("r.regionid").alias("region_id"),
        F.col("r.shortname").alias("region_name"),
        F.col("r.dnoregion").alias("dno_region"),
        F.col("r.intensity.forecast").alias("forecast_gco2_kwh"),
        F.col("r.intensity.index").alias("intensity_index"),
        "extracted_at",
        "run_id",
        "_source_file",
    )


def flatten_regional_generation(bronze: DataFrame) -> DataFrame:
    """One row per (region, half-hour, fuel)."""
    periods = bronze.select(*_lineage(), F.explode("payload.data").alias("p"))
    regions = periods.select("*", F.explode("p.regions").alias("r"))
    fuels = regions.select("*", F.explode("r.generationmix").alias("g"))
    return fuels.select(
        F.to_timestamp(F.col("p.from"), API_TS).alias("period_start_utc"),
        F.col("r.regionid").alias("region_id"),
        F.lower(F.col("g.fuel")).alias("fuel"),
        F.col("g.perc").alias("perc"),
        "extracted_at",
        "run_id",
        "_source_file",
    )


def flatten_national_intensity(bronze: DataFrame) -> DataFrame:
    """One row per half-hour. `actual` is null until after the period ends."""
    periods = bronze.select(*_lineage(), F.explode("payload.data").alias("p"))
    return periods.select(
        F.to_timestamp(F.col("p.from"), API_TS).alias("period_start_utc"),
        F.to_timestamp(F.col("p.to"), API_TS).alias("period_end_utc"),
        F.col("p.intensity.forecast").alias("forecast_gco2_kwh"),
        F.col("p.intensity.actual").alias("actual_gco2_kwh"),
        F.col("p.intensity.index").alias("intensity_index"),
        "extracted_at",
        "run_id",
        "_source_file",
    )


def flatten_national_generation(bronze: DataFrame) -> DataFrame:
    periods = bronze.select(*_lineage(), F.explode("payload.data").alias("p"))
    fuels = periods.select("*", F.explode("p.generationmix").alias("g"))
    return fuels.select(
        F.to_timestamp(F.col("p.from"), API_TS).alias("period_start_utc"),
        F.lower(F.col("g.fuel")).alias("fuel"),
        F.col("g.perc").alias("perc"),
        "extracted_at",
        "run_id",
        "_source_file",
    )


# --- Gold ---------------------------------------------------------------------
def add_uk_time_columns(df: DataFrame, ts_col: str = "period_start_utc") -> DataFrame:
    """Add Europe/London local-time columns. half_hour_slot is 0-47 (0 = 00:00 UK time)."""
    local = F.from_utc_timestamp(F.col(ts_col), UK_TZ)
    return (
        df.withColumn("period_start_uk", local)
        .withColumn("date_uk", F.to_date(local))
        .withColumn("hour_uk", F.hour(local))
        .withColumn("half_hour_slot", F.hour(local) * 2 + (F.minute(local) >= 30).cast("int"))
        .withColumn("day_of_week", F.date_format(local, "E"))
        .withColumn("day_of_week_num", F.dayofweek(local))  # 1 = Sunday
        .withColumn("month_uk", F.month(local))
    )


def pivot_fuel_mix(mix_long: DataFrame, keys: list[str]) -> DataFrame:
    """Pivot (key, fuel, perc) to one <fuel>_pct column per fuel, plus grouped shares.

    The explicit FUELS list keeps the output schema stable when a fuel is absent.
    """
    wide = mix_long.groupBy(*keys).pivot("fuel", FUELS).agg(F.first("perc"))
    for fuel in FUELS:
        wide = wide.withColumnRenamed(fuel, f"{fuel}_pct")

    def total(fuels: list[str]):
        return sum((F.coalesce(F.col(f"{f}_pct"), F.lit(0.0)) for f in fuels), F.lit(0.0))

    return (
        wide.withColumn("renewable_pct", F.round(total(RENEWABLE_FUELS), 1))
        .withColumn("low_carbon_pct", F.round(total(LOW_CARBON_FUELS), 1))
        .withColumn("fossil_pct", F.round(total(FOSSIL_FUELS), 1))
    )


def build_regional_fact(intensity: DataFrame, mix_wide: DataFrame, dim_region: DataFrame) -> DataFrame:
    """One row per region and half-hour with intensity, fuel mix and region attributes."""
    fact = (
        intensity.select("period_start_utc", "period_end_utc", "region_id", "forecast_gco2_kwh", "intensity_index")
        .join(mix_wide, ["period_start_utc", "region_id"], "left")
        .join(dim_region.select("region_id", "region_name", "country", "is_aggregate"), "region_id", "left")
    )
    return add_uk_time_columns(fact)


def build_region_daily_summary(fact: DataFrame) -> DataFrame:
    """Daily stats per region (UK calendar day)."""
    return (
        fact.groupBy("date_uk", "region_id", "region_name", "country", "is_aggregate")
        .agg(
            F.round(F.avg("forecast_gco2_kwh"), 1).alias("avg_gco2_kwh"),
            F.min("forecast_gco2_kwh").alias("min_gco2_kwh"),
            F.max("forecast_gco2_kwh").alias("max_gco2_kwh"),
            F.round(F.avg("renewable_pct"), 1).alias("avg_renewable_pct"),
            F.round(F.avg("wind_pct"), 1).alias("avg_wind_pct"),
            F.round(F.avg("solar_pct"), 1).alias("avg_solar_pct"),
            F.sum(F.when(F.col("intensity_index").isin("very low", "low"), 1).otherwise(0)).alias("low_carbon_periods"),
            F.count("*").alias("periods"),
        )
    )


def build_greenest_slots(fact: DataFrame, lookback_days: int = 90) -> DataFrame:
    """Average intensity by region, weekday and half-hour slot over the last N days.

    rank_in_day = 1 is the greenest slot for that region and weekday.
    """
    recent = fact.where(F.col("date_uk") >= F.date_sub(F.current_date(), lookback_days))
    agg = recent.groupBy("region_id", "region_name", "day_of_week", "day_of_week_num", "half_hour_slot").agg(
        F.round(F.avg("forecast_gco2_kwh"), 1).alias("avg_gco2_kwh"),
        F.count("*").alias("sample_size"),
    )
    w = Window.partitionBy("region_id", "day_of_week_num").orderBy("avg_gco2_kwh")
    return agg.withColumn("rank_in_day", F.row_number().over(w)).withColumn(
        "slot_label",
        F.format_string("%02d:%02d", (F.col("half_hour_slot") / 2).cast("int"), (F.col("half_hour_slot") % 2) * 30),
    )


def build_forecast_accuracy(national: DataFrame) -> DataFrame:
    """Daily MAE, MAPE and bias of the NESO national forecast against actuals.

    bias = mean(forecast - actual); positive means over-forecast.
    """
    scored = add_uk_time_columns(national.where(F.col("actual_gco2_kwh").isNotNull())).withColumn(
        "error", F.col("forecast_gco2_kwh") - F.col("actual_gco2_kwh")
    )
    return scored.groupBy("date_uk").agg(
        F.round(F.avg(F.abs("error")), 2).alias("mae_gco2_kwh"),
        F.round(
            F.avg(F.when(F.col("actual_gco2_kwh") > 0, F.abs("error") / F.col("actual_gco2_kwh") * 100)), 2
        ).alias("mape_pct"),
        F.round(F.avg("error"), 2).alias("bias_gco2_kwh"),
        F.round(F.avg("actual_gco2_kwh"), 1).alias("avg_actual_gco2_kwh"),
        F.count("*").alias("periods"),
    )
