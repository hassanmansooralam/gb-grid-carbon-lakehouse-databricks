# Genie space setup

Create a Genie space (sidebar → Genie → New) on the SQL warehouse and add these tables:

- `carbon_dev.gold.fact_regional_halfhour`
- `carbon_dev.gold.region_daily_summary`
- `carbon_dev.gold.greenest_slots`
- `carbon_dev.gold.national_halfhour`
- `carbon_dev.gold.national_forecast_accuracy`

## General instructions (paste into the space)

- Intensity is in gCO2/kWh. Lower is greener.
- Always use UK local time columns (`period_start_uk`, `date_uk`, `hour_uk`, `slot_label`) when the user mentions times.
- Exclude rows where `is_aggregate = true` unless the user asks about England, Scotland or Wales as a whole.
- "Renewable" means wind + solar + hydro. "Low carbon" adds nuclear. Biomass is excluded from both.
- For "greenest time" questions, use `gold.greenest_slots` and order by `avg_gco2_kwh`.

## Sample questions to add

- What's the greenest half-hour to charge an EV in London on a weekday?
- Which region had the lowest average intensity last month?
- How accurate was the NESO forecast last week?
- On which days this year was renewable share above 60% nationally?
