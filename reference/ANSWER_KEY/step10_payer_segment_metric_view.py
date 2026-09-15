# Databricks notebook source
# MAGIC %md
# MAGIC # Step 10: build the derived product, then expose it as a metric view (ANSWER KEY)
# MAGIC
# MAGIC In step 9 the room hits the wall: Genie space 1 cannot answer a revenue-by-payer-segment
# MAGIC question, because the certified data has no payer field. Step 10 adds that dimension as an
# MAGIC uncertified derived product, then wraps it in a Unity Catalog Metric View so Genie space 2
# MAGIC reads it as a governed semantic object, the same shape as the six pre-built metric views.
# MAGIC
# MAGIC This is the metric-view authoring moment. Two objects, both in `workshop_sandbox` (uncertified
# MAGIC until promoted), so they also reinforce the Level 1 trust ladder:
# MAGIC 1. `patient_payer_segment`: a synthetic payer segment derived from the primary channel
# MAGIC    (Durable Medical Equipment (DME) to Medicare, Pharmacy to Commercial, Direct-to-Consumer
# MAGIC    (DTC) to Cash-pay).
# MAGIC 2. `mv_payer_segment_revenue`: a metric view that joins certified revenue to that segment, so
# MAGIC    the new payer dimension carries certified revenue and margin measures.
# MAGIC
# MAGIC The pattern is copied from the pre-built `foundation/04_metric_views.py`. The join is on
# MAGIC `patient_id`; if step 5's deterministic-token mask is applied, the same token appears on both
# MAGIC sides, so the join still matches (that is the point of a deterministic token join key).

# COMMAND ----------

dbutils.widgets.text("catalog", "workshop_certified", "Primary catalog")
dbutils.widgets.text("sandbox_catalog", "", "Sandbox catalog (defaults to catalog)")
dbutils.widgets.text("schema", "workshop_analytics", "Certified analytics schema")
dbutils.widgets.text("sandbox_schema", "workshop_sandbox", "Uncertified sandbox schema")

catalog = dbutils.widgets.get("catalog")
sandbox_catalog = dbutils.widgets.get("sandbox_catalog") or catalog
schema = dbutils.widgets.get("schema")
sandbox_schema = dbutils.widgets.get("sandbox_schema")

analytics = f"{catalog}.{schema}"          # certified gold + metric views (read-only here)
sandbox = f"{sandbox_catalog}.{sandbox_schema}"  # the room's uncertified derived products

print(f"Certified source : {analytics}")
print(f"Sandbox target   : {sandbox}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10a. The derived data product: patient_payer_segment
# MAGIC
# MAGIC One row per patient, mapping the primary channel to a synthetic payer segment. It reads the
# MAGIC certified `gold_patients` in place, so Unity Catalog keeps a one-hop lineage back to a
# MAGIC certified source.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {sandbox}")

spark.sql(f"""
CREATE OR REPLACE TABLE {sandbox}.patient_payer_segment AS
SELECT
  patient_id,
  CASE channel_primary
    WHEN 'DME'      THEN 'Medicare'
    WHEN 'Pharmacy' THEN 'Commercial'
    WHEN 'DTC'      THEN 'Cash-pay'
    ELSE 'Unknown'
  END AS payer_segment
FROM {analytics}.gold_patients
""")
print(f"✓ Built derived product {sandbox}.patient_payer_segment")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10b. The metric view over it (the teaching moment)
# MAGIC
# MAGIC Same helper and shape as the pre-built metric views: drop any existing view of this name, then
# MAGIC `CREATE VIEW ... WITH METRICS LANGUAGE YAML`. The YAML `joins` block brings certified revenue
# MAGIC together with the derived payer segment, so the payer dimension carries certified measures.

# COMMAND ----------

def create_metric_view(fq_name: str, yaml_body: str):
    """Drop any existing view of this name, then create the metric view.
    CREATE OR REPLACE cannot swap a plain view for a metric view, so drop first for idempotency."""
    spark.sql(f"DROP VIEW IF EXISTS {fq_name}")
    spark.sql(f"CREATE VIEW {fq_name}\nWITH METRICS\nLANGUAGE YAML\nAS $$\n{yaml_body}\n$$")
    print(f"✓ Created metric view: {fq_name}")

create_metric_view(f"{sandbox}.mv_payer_segment_revenue", f"""
version: 1.1
source: {analytics}.gold_revenue_summary
comment: "Recognized revenue and margin by synthetic payer segment (uncertified derived product)."
joins:
  - name: seg
    source: {sandbox}.patient_payer_segment
    on: source.patient_id = seg.patient_id
dimensions:
  - name: Payer Segment
    expr: seg.payer_segment
  - name: Channel
    expr: channel
  - name: Quarter
    expr: quarter_num
  - name: Year
    expr: year_num
measures:
  - name: Patient Count
    expr: COUNT(DISTINCT patient_id)
  - name: Total Revenue USD
    expr: SUM(total_recognized_usd)
  - name: Total Margin USD
    expr: SUM(margin_usd)
  - name: Avg Margin Pct
    expr: AVG(gross_margin_pct)
""".strip())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification: the wall question is now answerable
# MAGIC
# MAGIC Revenue and margin by payer segment, the question Genie space 1 could not answer. Genie space 2
# MAGIC reads this metric view (with `MEASURE()`), so the previously unanswerable question is answered.

# COMMAND ----------

print("Revenue and margin by payer segment:")
spark.sql(f"""
SELECT `Payer Segment`,
       MEASURE(`Patient Count`)              AS patients,
       ROUND(MEASURE(`Total Revenue USD`))   AS revenue_usd,
       ROUND(MEASURE(`Avg Margin Pct`), 1)   AS avg_margin_pct
FROM {sandbox}.mv_payer_segment_revenue
GROUP BY ALL
ORDER BY revenue_usd DESC
""").show(truncate=False)

# Confirm it is a real METRIC_VIEW, not a plain view.
kind = spark.sql(f"DESCRIBE TABLE EXTENDED {sandbox}.mv_payer_segment_revenue") \
    .filter("col_name = 'Type'").collect()
print(f"✓ mv_payer_segment_revenue type: {kind[0]['data_type'] if kind else '?'}")
print("\n✓ Step 10 complete: derived payer segment + metric view; space 2 can now answer the wall question.")
