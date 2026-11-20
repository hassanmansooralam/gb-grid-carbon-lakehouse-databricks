-- Bar chart: regions ranked by average intensity this year
SELECT region_name, country,
       ROUND(AVG(avg_gco2_kwh), 1)      AS avg_gco2_kwh,
       ROUND(AVG(avg_renewable_pct), 1) AS avg_renewable_pct
FROM carbon_dev.gold.region_daily_summary
WHERE NOT is_aggregate
  AND year(date_uk) = year(current_date())
GROUP BY region_name, country
ORDER BY avg_gco2_kwh;
