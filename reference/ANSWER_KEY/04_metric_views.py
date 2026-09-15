# Databricks notebook source
# MAGIC %md
# MAGIC # Metric Views: Five Views for Genie Space (Phase 3)
# MAGIC
# MAGIC Creates the five metric views that back the Genie space:
# MAGIC 1. mv_revenue_by_channel — revenue by channel, product, quarter
# MAGIC 2. mv_fulfillment_rate — PSA-to-Pump-on-Body success rate, cycle time
# MAGIC 3. mv_recognized_revenue — per-patient revenue recognition timeline
# MAGIC 4. mv_recurring_revenue — consumable revenue cohorts by device and channel
# MAGIC 5. mv_gross_margin — margin by channel and device
# MAGIC
# MAGIC All views source GOLD tables only (certified data, governance-ready).

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workshop_certified", "Primary catalog")
dbutils.widgets.text("schema", "analytics", "Schema name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

print(f"Building metric views in {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. mv_revenue_by_channel

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.mv_revenue_by_channel AS
SELECT
  r.channel,
  r.device_type,
  r.quarter_num as quarter,
  r.year_num as year,
  COUNT(DISTINCT r.patient_id) as patient_count,
  ROUND(SUM(r.upfront_recognized_usd), 2) as total_upfront_usd,
  ROUND(SUM(r.recurring_recognized_usd), 2) as total_recurring_usd,
  ROUND(SUM(r.total_recognized_usd), 2) as total_revenue_usd,
  ROUND(AVG(r.gross_margin_pct), 2) as avg_margin_pct,
  ROUND(SUM(r.margin_usd), 2) as total_margin_usd
FROM {catalog}.{schema}.gold_revenue_summary r
WHERE r.is_break_even = true
GROUP BY r.channel, r.device_type, r.quarter_num, r.year_num
ORDER BY r.quarter_num DESC, r.year_num DESC, r.channel
""")

print("✓ Created mv_revenue_by_channel")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. mv_fulfillment_rate
# MAGIC
# MAGIC Exposes cycle time by channel and device for oracle query 1.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.mv_fulfillment_rate AS
WITH cohort_counts AS (
  -- Distinct counts done in GROUP BY (Spark does not support COUNT(DISTINCT ...) as a window function)
  SELECT
    channel, quarter_num, year_num,
    COUNT(DISTINCT CASE WHEN claim_type = 'pump_on_body' THEN patient_id END) as pump_on_body_count,
    COUNT(DISTINCT patient_id) as claim_event_count
  FROM {catalog}.{schema}.gold_claims
  WHERE claim_type IN ('pump_on_body', 'consumable_shipment')
  GROUP BY channel, quarter_num, year_num
)
SELECT
  c.channel,
  c.device_type,
  c.quarter_num as quarter,
  c.year_num as year,
  c.claim_date,
  c.claim_type,
  c.patient_id,
  c.days_to_pump_on_body,
  cc.pump_on_body_count,
  cc.claim_event_count
FROM {catalog}.{schema}.gold_claims c
LEFT JOIN cohort_counts cc
  ON c.channel = cc.channel AND c.quarter_num = cc.quarter_num AND c.year_num = cc.year_num
WHERE c.claim_type IN ('pump_on_body', 'consumable_shipment')
ORDER BY c.claim_date DESC
""")

print("✓ Created mv_fulfillment_rate")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. mv_recognized_revenue
# MAGIC
# MAGIC Per-patient, per-month revenue recognition (upfront + recurring).

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.mv_recognized_revenue AS
SELECT
  r.patient_id,
  r.month,
  r.channel,
  r.device_type,
  r.region,
  r.quarter_num as quarter,
  r.year_num as year,
  ROUND(r.upfront_recognized_usd, 2) as upfront_usd,
  ROUND(r.recurring_recognized_usd, 2) as recurring_usd,
  ROUND(r.total_recognized_usd, 2) as total_revenue_usd,
  ROUND(r.margin_usd, 2) as margin_usd,
  ROUND(r.gross_margin_pct, 2) as margin_pct,
  r.is_break_even
FROM {catalog}.{schema}.gold_revenue_summary r
ORDER BY r.month DESC, r.patient_id
""")

print("✓ Created mv_recognized_revenue")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. mv_recurring_revenue
# MAGIC
# MAGIC 12-month consumable revenue cohorts by device type and channel.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.mv_recurring_revenue AS
SELECT
  r.channel,
  r.device_type,
  p.patient_lifecycle_status,
  COUNT(DISTINCT r.patient_id) as patient_cohort_size,
  ROUND(SUM(r.recurring_recognized_usd), 2) as total_consumable_revenue_12m,
  ROUND(AVG(r.recurring_recognized_usd), 2) as avg_monthly_consumable_shipment,
  COUNT(DISTINCT r.month) as shipment_months,
  ROUND(AVG(r.gross_margin_pct), 2) as avg_margin_pct
FROM {catalog}.{schema}.gold_revenue_summary r
JOIN {catalog}.{schema}.gold_patients p ON r.patient_id = p.patient_id
WHERE r.recurring_recognized_usd > 0
GROUP BY r.channel, r.device_type, p.patient_lifecycle_status
ORDER BY r.channel, r.device_type
""")

print("✓ Created mv_recurring_revenue")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. mv_gross_margin
# MAGIC
# MAGIC Exposes margin by channel and device for oracle query 2.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.mv_gross_margin AS
SELECT
  r.channel,
  r.device_type,
  r.region,
  r.month,
  r.quarter_num as quarter,
  r.year_num as year,
  COUNT(DISTINCT r.patient_id) as patient_count,
  ROUND(SUM(r.total_recognized_usd), 2) as revenue_usd,
  ROUND(SUM(r.margin_usd), 2) as margin_usd,
  ROUND(AVG(r.gross_margin_pct), 2) as gross_margin_pct,
  ROUND(SUM(r.fulfillment_cost_usd), 2) as fulfillment_cost_usd
FROM {catalog}.{schema}.gold_revenue_summary r
WHERE r.is_break_even = true
GROUP BY r.channel, r.device_type, r.region, r.month, r.quarter_num, r.year_num
ORDER BY r.month DESC, r.channel, r.device_type
""")

print("✓ Created mv_gross_margin")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification: All 5 views created

# COMMAND ----------

views = [
    "mv_revenue_by_channel",
    "mv_fulfillment_rate",
    "mv_recognized_revenue",
    "mv_recurring_revenue",
    "mv_gross_margin"
]

for view_name in views:
    count = spark.sql(f"SELECT COUNT(*) FROM {catalog}.{schema}.{view_name}").collect()[0][0]
    print(f"✓ {view_name:30s}: {count:>6,} rows")

print("\n✓ All metric views created!")
