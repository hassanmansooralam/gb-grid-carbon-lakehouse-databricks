"""Batch-score the next 24h with the @champion model.

Appends predictions to <catalog>.ml.forecast_predictions so each forecast can later
be compared with the actual value.
"""

from __future__ import annotations

import argparse
import os
import sys


def _add_src_to_path() -> None:
    try:
        here = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        here = os.path.dirname(os.path.abspath(sys.argv[0]))
    sys.path.insert(0, os.path.abspath(os.path.join(here, "..")))


_add_src_to_path()

import mlflow  # noqa: E402
import pandas as pd  # noqa: E402
from mlflow.tracking import MlflowClient  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from carbon_lakehouse.features import FEATURE_COLUMNS, TARGET, build_features, extend_for_scoring  # noqa: E402

HISTORY_DAYS = 21


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    args = p.parse_args()

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    model_name = f"{args.catalog}.ml.carbon_intensity_forecaster"
    out_table = f"{args.catalog}.ml.forecast_predictions"

    mlflow.set_registry_uri("databricks-uc")
    try:
        version = MlflowClient().get_model_version_by_alias(model_name, "champion").version
    except Exception:
        print(f"No @champion for {model_name} yet - run the training job first. Skipping.")
        return
    model = mlflow.pyfunc.load_model(f"models:/{model_name}@champion")

    nat = spark.read.table(f"{args.catalog}.gold.national_halfhour")
    history = (
        nat.where(F.col("actual_gco2_kwh").isNotNull())
        .where(F.col("period_start_utc") >= F.expr(f"current_timestamp() - INTERVAL {HISTORY_DAYS} DAYS"))
        .select("period_start_utc", "actual_gco2_kwh")
        .toPandas()
    )
    if history.empty:
        print("No recent actuals - nothing to score.")
        return

    last_actual = pd.to_datetime(history["period_start_utc"], utc=True).max()
    feats = build_features(extend_for_scoring(history))
    future = feats[feats["period_start_utc"] > last_actual].copy()
    future["predicted_gco2_kwh"] = model.predict(future[FEATURE_COLUMNS]).round(1)

    preds = spark.createDataFrame(future[["period_start_utc", "predicted_gco2_kwh"]])
    neso = nat.select("period_start_utc", F.col("forecast_gco2_kwh").alias("neso_forecast_gco2_kwh"))
    result = (
        preds.join(neso, "period_start_utc", "left")
        .withColumn("model_name", F.lit(model_name))
        .withColumn("model_version", F.lit(str(version)))
        .withColumn("scored_at", F.current_timestamp())
    )

    result.write.mode("append").option("mergeSchema", "true").saveAsTable(out_table)
    print(f"Wrote {result.count()} predictions with {model_name} v{version} to {out_table} ({TARGET} target)")


if __name__ == "__main__":
    main()
