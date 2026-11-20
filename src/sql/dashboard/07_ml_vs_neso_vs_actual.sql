-- Line chart: model vs NESO vs actual. Uses each period's most recent prediction.
WITH latest_pred AS (
  SELECT *
  FROM carbon_dev.ml.forecast_predictions
  QUALIFY ROW_NUMBER() OVER (PARTITION BY period_start_utc ORDER BY scored_at DESC) = 1
)
SELECT p.period_start_utc,
       p.predicted_gco2_kwh       AS our_model,
       p.neso_forecast_gco2_kwh   AS neso_forecast,
       n.actual_gco2_kwh          AS actual,
       p.model_version
FROM latest_pred p
LEFT JOIN carbon_dev.gold.national_halfhour n USING (period_start_utc)
WHERE p.period_start_utc >= current_timestamp() - INTERVAL 14 DAYS
ORDER BY p.period_start_utc;
