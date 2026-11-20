-- Manual alternative to infra/terraform. Run in the SQL editor if you want to
-- get going before setting up Terraform. Repeat with carbon_prod for prod.

CREATE CATALOG IF NOT EXISTS carbon_dev
  COMMENT 'GB Grid Carbon Lakehouse - dev. Source: NESO Carbon Intensity API (CC BY 4.0).';

CREATE SCHEMA IF NOT EXISTS carbon_dev.raw    COMMENT 'Landing zone volume';
CREATE SCHEMA IF NOT EXISTS carbon_dev.bronze COMMENT 'Raw API envelopes';
CREATE SCHEMA IF NOT EXISTS carbon_dev.silver COMMENT 'Flattened, validated, de-duplicated';
CREATE SCHEMA IF NOT EXISTS carbon_dev.gold   COMMENT 'Business-ready materialized views';
CREATE SCHEMA IF NOT EXISTS carbon_dev.ml     COMMENT 'Models and predictions';

CREATE VOLUME IF NOT EXISTS carbon_dev.raw.landing
  COMMENT 'Raw JSON from the extractor';

-- If CREATE CATALOG is not allowed in your workspace, use the built-in
-- `workspace` catalog instead: create the schemas there and set
-- catalog: workspace in databricks.yml.
