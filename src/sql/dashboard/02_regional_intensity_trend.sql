-- Line chart: daily average intensity per region (DNO regions only, last 90 days)
SELECT date_uk, region_name, avg_gco2_kwh, avg_renewable_pct
FROM carbon_dev.gold.region_daily_summary
WHERE NOT is_aggregate
  AND date_uk >= current_date() - INTERVAL 90 DAYS
ORDER BY date_uk, region_name;
