"""Shared pytest fixtures: a local Spark session and bronze fixture loader."""

import os
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    # PySpark converts naive datetimes using the local timezone; pin to UTC.
    # On Windows, set TZ=UTC0 in the shell before running pytest.
    os.environ["TZ"] = "UTC"
    if hasattr(time, "tzset"):
        time.tzset()
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    session = (
        SparkSession.builder.master("local[1]")
        .appName("carbon-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def bronze(spark):
    from pyspark.sql import functions as F

    from carbon_lakehouse.transforms import BRONZE_SCHEMAS

    def _load(endpoint: str):
        return (
            spark.read.schema(BRONZE_SCHEMAS[endpoint])
            .option("multiLine", "true")
            .json(str(FIXTURES / f"{endpoint}.json"))
            .withColumn("_source_file", F.lit(f"{endpoint}.json"))
        )

    return _load
