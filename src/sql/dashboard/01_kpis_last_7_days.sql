-- KPI tiles: national picture over the last 7 days
SELECT
  ROUND(AVG(actual_gco2_kwh), 0)                       AS avg_actual_gco2_kwh,
  MIN(actual_gco2_kwh)                                  AS min_actual_gco2_kwh,
  MAX(actual_gco2_kwh)                                  AS max_actual_gco2_kwh,
  ROUND(AVG(renewable_pct), 1)                          AS avg_renewable_pct,
  ROUND(AVG(ABS(forecast_gco2_kwh - actual_gco2_kwh)), 1) AS neso_mae_gco2_kwh
FROM carbon_dev.gold.national_halfhour
WHERE actual_gco2_kwh IS NOT NULL
  AND date_uk >= current_date() - INTERVAL 7 DAYS;
