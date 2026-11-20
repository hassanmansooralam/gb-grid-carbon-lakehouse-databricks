"""Bronze: raw landing files to append-only streaming tables via Auto Loader."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from carbon_lakehouse.transforms import BRONZE_SCHEMAS

LANDING_ROOT = spark.conf.get("landing_root")  # noqa: F821


def make_bronze_table(endpoint: str):
    @dp.table(
        name=f"bronze.{endpoint}_raw",
        comment=f"Raw API envelopes for {endpoint}, one row per landed file.",
        table_properties={"quality": "bronze"},
    )
    @dp.expect_or_fail("has_payload", "payload IS NOT NULL")
    def _bronze():
        return (
            spark.readStream.format("cloudFiles")  # noqa: F821
            .option("cloudFiles.format", "json")
            .option("multiLine", "true")
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .schema(BRONZE_SCHEMAS[endpoint])
            .load(f"{LANDING_ROOT}/{endpoint}/")
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_ingested_at", F.current_timestamp())
        )

    return _bronze


for _endpoint in BRONZE_SCHEMAS:
    make_bronze_table(_endpoint)
