-- Line chart: daily MAE and bias of the official NESO forecast
SELECT date_uk, mae_gco2_kwh, mape_pct, bias_gco2_kwh
FROM carbon_dev.gold.national_forecast_accuracy
WHERE date_uk >= current_date() - INTERVAL 180 DAYS
ORDER BY date_uk;
