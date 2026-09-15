# Databricks notebook source
# MAGIC %md
# MAGIC # Step 10: build the missing dimension, then expose it as a metric view
# MAGIC
# MAGIC Genie space 1 hit the wall in step 9: it cannot answer a revenue-by-payer-segment question,
# MAGIC because the certified data has no payer field. Your job is to add that dimension and make it a
# MAGIC governed semantic object Genie can read, the same shape as the six metric views already in
# MAGIC `workshop_analytics`.
# MAGIC
# MAGIC Build two objects in `workshop_sandbox` (uncertified until promoted):
# MAGIC 1. **`patient_payer_segment`** — one row per patient, mapping the primary channel to a payer
# MAGIC    segment: DME to Medicare, Pharmacy to Commercial, DTC to Cash-pay. Read `gold_patients`
# MAGIC    in place (keep the lineage back to the certified source).
# MAGIC 2. **`mv_payer_segment_revenue`** — a Unity Catalog Metric View that joins certified revenue
# MAGIC    (`gold_revenue_summary`) to your payer segment on `patient_id`, with `Payer Segment` as a
# MAGIC    dimension and revenue + margin measures.
# MAGIC
# MAGIC Acceptance: `SELECT MEASURE(...) ... GROUP BY \`Payer Segment\`` returns revenue by segment, and
# MAGIC `DESCRIBE TABLE EXTENDED` reports type `METRIC_VIEW`. Genie space 2 then answers the wall question.
# MAGIC
# MAGIC Hint: the pattern is already written for you in the pre-built `foundation/04_metric_views.py`
# MAGIC (the `create_metric_view` helper and the YAML `joins` block in `mv_recurring_revenue`). Copy it.

# COMMAND ----------

dbutils.widgets.text("catalog", "workshop_certified", "Primary catalog")
dbutils.widgets.text("sandbox_catalog", "", "Sandbox catalog (defaults to catalog)")
dbutils.widgets.text("schema", "workshop_analytics", "Certified analytics schema")
dbutils.widgets.text("sandbox_schema", "workshop_sandbox", "Uncertified sandbox schema")

catalog = dbutils.widgets.get("catalog")
sandbox_catalog = dbutils.widgets.get("sandbox_catalog") or catalog
schema = dbutils.widgets.get("schema")
sandbox_schema = dbutils.widgets.get("sandbox_schema")

analytics = f"{catalog}.{schema}"
sandbox = f"{sandbox_catalog}.{sandbox_schema}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10a. TODO: build patient_payer_segment
# MAGIC
# MAGIC One row per patient: `patient_id`, `payer_segment` (from `gold_patients.channel_primary`).
# MAGIC Target: `{sandbox}.patient_payer_segment`.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {sandbox}")

# TODO: CREATE OR REPLACE TABLE {sandbox}.patient_payer_segment AS
#   SELECT patient_id, <CASE channel_primary -> payer_segment> FROM {analytics}.gold_patients

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10b. TODO: build the metric view mv_payer_segment_revenue
# MAGIC
# MAGIC A `CREATE VIEW ... WITH METRICS LANGUAGE YAML` over `{analytics}.gold_revenue_summary`, with a
# MAGIC YAML `joins` entry to `{sandbox}.patient_payer_segment` on `patient_id`. Dimension: `Payer
# MAGIC Segment`. Measures: total revenue and average margin percent. Copy the shape from
# MAGIC `foundation/04_metric_views.py`.

# COMMAND ----------

# TODO: DROP VIEW IF EXISTS {sandbox}.mv_payer_segment_revenue, then
#   CREATE VIEW {sandbox}.mv_payer_segment_revenue WITH METRICS LANGUAGE YAML AS $$ ... $$
#   (source: gold_revenue_summary; joins: patient_payer_segment on patient_id; dimension Payer Segment)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify (this is your acceptance check)

# COMMAND ----------

# TODO: SELECT `Payer Segment`, MEASURE(`Total Revenue USD`) ... GROUP BY ALL ORDER BY 2 DESC
#       and confirm DESCRIBE TABLE EXTENDED reports type METRIC_VIEW.
