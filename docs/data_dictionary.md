# Data dictionary

Intensity is in **gCO2/kWh** (grams of CO2 per kilowatt-hour). Lower is greener. All `*_utc` timestamps are UTC, and `*_uk` columns are Europe/London local time (with BST).

## Silver

### silver.regional_intensity (key: region_id, period_start_utc)
| Column | Type | Description |
|---|---|---|
| period_start_utc / period_end_utc | timestamp | Half-hour window |
| region_id | int | 1–14 DNO regions, 15 England, 16 Scotland, 17 Wales |
| region_name, dno_region | string | Short name; network operator name |
| forecast_gco2_kwh | int | Latest NESO forecast |
| intensity_index | string | very low / low / moderate / high / very high |
| extracted_at, run_id, _source_file | | Lineage back to the bronze file |

### silver.regional_generation_mix (key: region_id, period_start_utc, fuel)
`fuel` (biomass, coal, gas, hydro, imports, nuclear, oil, other, solar, wind) and `perc` (0–100).

### silver.national_intensity (key: period_start_utc)
`forecast_gco2_kwh`, `actual_gco2_kwh` (null until after the period, then filled in), `intensity_index`.

### silver.national_generation_mix (key: period_start_utc, fuel)
### silver.dim_region
`region_id`, `region_name`, `country`, `is_aggregate` (true for 15–17; exclude these when summing regions).

## Gold

### gold.fact_regional_halfhour
Silver intensity, plus `<fuel>_pct` for each fuel, plus:
- `renewable_pct` = wind + solar + hydro
- `low_carbon_pct` = renewable + nuclear
- `fossil_pct` = gas + coal + oil
- region attributes
- UK time columns: `period_start_uk`, `date_uk`, `hour_uk`, `half_hour_slot` (0–47), `day_of_week`, `day_of_week_num` (1 = Sun), `month_uk`

### gold.region_daily_summary
Per `date_uk` and region: `avg/min/max_gco2_kwh`, `avg_renewable_pct`, `avg_wind_pct`, `avg_solar_pct`, `low_carbon_periods` (index very low/low), `periods`.

### gold.greenest_slots
Per region × weekday × `half_hour_slot` over the last 90 days: `avg_gco2_kwh`, `sample_size`, `rank_in_day` (1 = greenest), `slot_label` ("13:30").

### gold.national_halfhour
National forecast + actual + national fuel mix % + UK time columns.

### gold.national_forecast_accuracy
Per `date_uk`: `mae_gco2_kwh`, `mape_pct`, `bias_gco2_kwh` (positive = NESO over-forecast), `avg_actual_gco2_kwh`, `periods`.

## ML

### ml.forecast_predictions (appended)
`period_start_utc`, `predicted_gco2_kwh`, `neso_forecast_gco2_kwh`, `model_name`, `model_version`, `scored_at`.
