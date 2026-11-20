"""Silver: flatten, validate and de-duplicate.

Each extract re-pulls an overlap window, so bronze holds several versions of the
same half-hour. AUTO CDC (SCD type 1) keeps the latest version per key, ordered by
extraction time; bronze retains every version for audit.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from carbon_lakehouse import transforms as T
from carbon_lakehouse.reference import REGIONS

VALID_PERIOD = {"valid_period": "period_start_utc IS NOT NULL"}
VALID_INTENSITY = {"intensity_in_range": "forecast_gco2_kwh BETWEEN 0 AND 1000"}
VALID_REGION = {"valid_region": "region_id BETWEEN 1 AND 17"}
VALID_PERC = {"perc_in_range": "perc BETWEEN 0 AND 100", "fuel_present": "fuel IS NOT NULL"}


def scd1(target: str, source: str, keys: list[str], comment: str, cluster_by: list[str]):
    dp.create_streaming_table(
        name=target, comment=comment, cluster_by=cluster_by, table_properties={"quality": "silver"}
    )
    dp.create_auto_cdc_flow(
        target=target,
        source=source,
        keys=keys,
        sequence_by=F.col("extracted_at"),
        stored_as_scd_type=1,
    )


@dp.temporary_view(name="v_regional_intensity")
@dp.expect_all_or_drop({**VALID_PERIOD, **VALID_INTENSITY, **VALID_REGION})
def v_regional_intensity():
    return T.flatten_regional_intensity(spark.readStream.table("bronze.regional_intensity_raw"))  # noqa: F821


scd1(
    "silver.regional_intensity",
    "v_regional_intensity",
    keys=["region_id", "period_start_utc"],
    comment="Latest forecast carbon intensity per GB region and half-hour (UTC).",
    cluster_by=["period_start_utc", "region_id"],
)


@dp.temporary_view(name="v_regional_generation")
@dp.expect_all_or_drop({**VALID_PERIOD, **VALID_REGION, **VALID_PERC})
def v_regional_generation():
    return T.flatten_regional_generation(spark.readStream.table("bronze.regional_intensity_raw"))  # noqa: F821


scd1(
    "silver.regional_generation_mix",
    "v_regional_generation",
    keys=["region_id", "period_start_utc", "fuel"],
    comment="Latest generation mix % by fuel, per region and half-hour.",
    cluster_by=["period_start_utc", "region_id"],
)


@dp.temporary_view(name="v_national_intensity")
@dp.expect_all_or_drop({**VALID_PERIOD, **VALID_INTENSITY})
@dp.expect("actual_in_range", "actual_gco2_kwh IS NULL OR actual_gco2_kwh BETWEEN 0 AND 1000")
def v_national_intensity():
    return T.flatten_national_intensity(spark.readStream.table("bronze.national_intensity_raw"))  # noqa: F821


scd1(
    "silver.national_intensity",
    "v_national_intensity",
    keys=["period_start_utc"],
    comment="National forecast and actual intensity per half-hour.",
    cluster_by=["period_start_utc"],
)


@dp.temporary_view(name="v_national_generation")
@dp.expect_all_or_drop({**VALID_PERIOD, **VALID_PERC})
def v_national_generation():
    return T.flatten_national_generation(spark.readStream.table("bronze.national_generation_raw"))  # noqa: F821


scd1(
    "silver.national_generation_mix",
    "v_national_generation",
    keys=["period_start_utc", "fuel"],
    comment="National generation mix % by fuel per half-hour.",
    cluster_by=["period_start_utc"],
)


@dp.materialized_view(name="silver.dim_region", comment="GB DNO regions (1-14) and country roll-ups (15-17).")
def dim_region():
    return spark.createDataFrame(  # noqa: F821
        REGIONS, "region_id int, region_name string, country string, is_aggregate boolean"
    )
