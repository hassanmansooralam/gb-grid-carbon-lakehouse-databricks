# Runbook

## Routine
- **Hourly:** GitHub Actions `extract` lands files, the file arrival trigger runs `carbon_refresh` (pipeline, then scoring).
- **Mondays 06:30 UK:** `carbon_train` retrains, promoting the new model to `@champion` only if it has a lower test MAE.

## Backfill a date range
Actions tab → `extract` → Run workflow → set start/end (e.g. `2024-01-01` to `2024-07-01`). The watermark never moves backwards, so this is safe. Old dates load straight through bronze, and AUTO CDC slots them into silver.

## Full rebuild of silver/gold
Pipeline UI → **Full refresh all**. Bronze is reprocessed from the files still in the volume. Do this after changing flatten logic.

## Something failed

| Symptom | Likely cause | Fix |
|---|---|---|
| `extract` fails with 4xx | API path changed or date range too long | Check `endpoints.yml`, lower `chunk_days` |
| `extract` fails with 401/403 | Token expired | Rotate the PAT, update the GitHub secret |
| Bronze fails on `has_payload` | Corrupt or empty file | Inspect `_source_file`, move the file out of landing, re-run |
| Many rows dropped in silver | API returned nulls or out-of-range values | Pipeline UI → data quality tab; query bronze for that period |
| `ModuleNotFoundError: carbon_lakehouse` in pipeline | `root_path` not applied | Pipeline settings → check root folder is `src` |
| Scoring says "No @champion" | Training job hasn't run yet | `databricks bundle run carbon_train` |
| Compute unavailable for the day | Free Edition daily quota exceeded | Wait for reset; reduce backfill size or trigger frequency |

## Build the dashboard
1. SQL editor: run each file in `src/sql/dashboard/` (change `carbon_dev` if needed).
2. Dashboards → Create → add each query as a dataset. Suggested visuals: KPI counters (01), line (02, 06, 07), heatmap with a region filter (03), bar (04), scatter (05).
3. Publish, then export it into the repo so it's version-controlled:
   `databricks bundle generate dashboard --existing-path "/Workspace/Users/<you>/GB Grid Carbon.lvdash.json"`
