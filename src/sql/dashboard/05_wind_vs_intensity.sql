-- Scatter: does more wind mean lower intensity? (national, last 90 days)
SELECT wind_pct, actual_gco2_kwh, hour_uk
FROM carbon_dev.gold.national_halfhour
WHERE actual_gco2_kwh IS NOT NULL
  AND date_uk >= current_date() - INTERVAL 90 DAYS;
