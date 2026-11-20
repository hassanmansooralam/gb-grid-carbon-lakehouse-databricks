"""Train the 24h-ahead national carbon intensity model.

Logs to MLflow, registers the model in Unity Catalog and promotes it to @champion
only if its test MAE beats the current champion. Benchmarks against a persistence
baseline (lag_48) and the NESO forecast.
"""

from __future__ import annotations

import argparse
import os
import sys


def _add_src_to_path() -> None:
    """Make the carbon_lakehouse package importable inside a Databricks Python task."""
    try:
        here = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        here = os.path.dirname(os.path.abspath(sys.argv[0]))
    sys.path.insert(0, os.path.abspath(os.path.join(here, "..")))


_add_src_to_path()

import mlflow  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from mlflow.models import infer_signature  # noqa: E402
from mlflow.tracking import MlflowClient  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from sklearn.ensemble import HistGradientBoostingRegressor  # noqa: E402
from sklearn.metrics import mean_absolute_error  # noqa: E402

from carbon_lakehouse.features import FEATURE_COLUMNS, TARGET, build_features, time_split  # noqa: E402


def mae(y_true, y_pred) -> float:
    return float(mean_absolute_error(y_true, y_pred))


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--experiment", default="/Shared/gb-grid-carbon-forecast")
    p.add_argument("--test-days", type=int, default=30)
    args = p.parse_args()

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    model_name = f"{args.catalog}.ml.carbon_intensity_forecaster"

    pdf = (
        spark.read.table(f"{args.catalog}.gold.national_halfhour")
        .select("period_start_utc", "actual_gco2_kwh", "forecast_gco2_kwh")
        .toPandas()
    )
    neso = pdf.set_index("period_start_utc")["forecast_gco2_kwh"]
    neso.index = pd.to_datetime(neso.index, utc=True)

    feats = build_features(pdf).dropna(subset=[TARGET, "lag_48"])
    train, test = time_split(feats, test_days=args.test_days)
    X_train, y_train = train[FEATURE_COLUMNS], train[TARGET]
    X_test, y_test = test[FEATURE_COLUMNS], test[TARGET]

    params = {"max_iter": 400, "learning_rate": 0.05, "max_leaf_nodes": 31, "l2_regularization": 1.0}
    model = HistGradientBoostingRegressor(random_state=42, **params)

    mlflow.set_registry_uri("databricks-uc")
    mlflow.set_experiment(args.experiment)

    with mlflow.start_run(run_name="hgb_24h_ahead") as run:
        model.fit(X_train, y_train)
        pred = model.predict(X_test)

        neso_test = neso.reindex(test["period_start_utc"]).to_numpy()
        mask = ~np.isnan(neso_test)

        metrics = {
            "test_mae": mae(y_test, pred),
            "naive_lag48_mae": mae(y_test, X_test["lag_48"]),
            "neso_mae": mae(y_test[mask], neso_test[mask]) if mask.any() else float("nan"),
            "train_rows": float(len(train)),
            "test_rows": float(len(test)),
        }
        mlflow.log_params({**params, "features": ",".join(FEATURE_COLUMNS), "test_days": args.test_days})
        mlflow.log_metrics(metrics)
        print(metrics)

        info = mlflow.sklearn.log_model(
            sk_model=model,
            artifact_path="model",
            signature=infer_signature(X_test, pred),
            input_example=X_test.head(5),
            registered_model_name=model_name,
        )
        new_version = str(info.registered_model_version)

    client = MlflowClient()
    try:
        champ = client.get_model_version_by_alias(model_name, "champion")
        champ_mae = client.get_run(champ.run_id).data.metrics.get("test_mae", float("inf"))
    except Exception:
        champ, champ_mae = None, float("inf")

    if metrics["test_mae"] < champ_mae:
        client.set_registered_model_alias(model_name, "champion", new_version)
        print(f"Promoted v{new_version} to @champion (MAE {metrics['test_mae']:.2f} < {champ_mae:.2f})")
    else:
        client.set_registered_model_alias(model_name, "challenger", new_version)
        print(f"Kept v{champ.version} as champion; v{new_version} tagged @challenger")

    print(f"run_id={run.info.run_id}")


if __name__ == "__main__":
    main()
