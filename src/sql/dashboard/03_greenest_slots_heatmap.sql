-- Heatmap: typical intensity by weekday x half-hour slot for a chosen region
-- Add a dashboard filter/parameter on region_name.
SELECT region_name, day_of_week, day_of_week_num, slot_label, half_hour_slot, avg_gco2_kwh, rank_in_day
FROM carbon_dev.gold.greenest_slots
ORDER BY region_name, day_of_week_num, half_hour_slot;
