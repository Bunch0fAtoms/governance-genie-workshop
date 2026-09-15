# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Deduplicate, Enrich, Quality Flags
# MAGIC
# MAGIC Transforms bronze → silver:
# MAGIC - Deduplicate on grain keys
# MAGIC - Add enrichment columns (age, device_category, days_to_pump_on_body, quarter, etc.)
# MAGIC - Add quality_score

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

dbutils.widgets.text("catalog", "workshop_certified", "Primary catalog")
dbutils.widgets.text("sandbox_catalog", "workshop_sandbox", "Sandbox catalog")
dbutils.widgets.text("schema", "analytics", "Schema name")

catalog = dbutils.widgets.get("catalog")
sandbox_catalog = dbutils.widgets.get("sandbox_catalog")
schema = dbutils.widgets.get("schema")

print(f"Building silver in {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: Patients (deduplicated, with age and enrichment)

# COMMAND ----------

silver_patients = spark.sql(f"""
WITH deduplicated AS (
  SELECT
    patient_id,
    device_type,
    region,
    ssn,
    dob,
    patient_name,
    enrolled_date,
    _loaded_at,
    _source,
    ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY _loaded_at DESC) as rn
  FROM {catalog}.{schema}.bronze_patients
)
SELECT
  patient_id,
  device_type,
  LOWER(CASE WHEN device_type = 'Mobi' THEN 'mobi' ELSE 'slim' END) as device_category,
  region,
  ssn,
  dob,
  patient_name,
  enrolled_date,
  CAST((DATEDIFF(enrolled_date, dob) / 365.25) AS INT) as patient_age_at_enrollment,
  _loaded_at,
  _source,
  1.0 as _quality_score
FROM deduplicated
WHERE rn = 1
""")

silver_patients.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
    f"{catalog}.{schema}.silver_patients"
)

print(f"✓ Silver patients: {silver_patients.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: Claims (with cycle time, quarter, and patient attributes)

# COMMAND ----------

silver_claims = spark.sql(f"""
WITH claim_cte AS (
  SELECT
    c.claim_id,
    c.patient_id,
    c.claim_date,
    c.claim_type,
    c.channel,
    c.product_line,
    c.product_sku,
    c.claim_amount_usd,
    c.units,
    c._loaded_at,
    c._source,
    p.device_type,
    p.region,
    QUARTER(c.claim_date) as quarter_num,
    YEAR(c.claim_date) as year_num
  FROM {catalog}.{schema}.bronze_claims c
  JOIN {catalog}.{schema}.silver_patients p ON c.patient_id = p.patient_id
),
psa_pump_join AS (
  SELECT
    c1.claim_id,
    c1.patient_id,
    c1.claim_date,
    c1.claim_type,
    c1.channel,
    c1.product_line,
    c1.product_sku,
    c1.claim_amount_usd,
    c1.units,
    c1._loaded_at,
    c1._source,
    c1.device_type,
    c1.region,
    c1.quarter_num,
    c1.year_num,
    DATEDIFF(c_pump.claim_date, c_psa.claim_date) as days_to_pump_on_body,
    CASE WHEN c1.claim_amount_usd > 500 THEN true ELSE false END as is_high_value
  FROM claim_cte c1
  LEFT JOIN claim_cte c_psa ON c1.patient_id = c_psa.patient_id AND c_psa.claim_type = 'psa_intake'
  LEFT JOIN claim_cte c_pump ON c1.patient_id = c_pump.patient_id AND c_pump.claim_type = 'pump_on_body'
)
SELECT *
FROM psa_pump_join
""")

silver_claims.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
    f"{catalog}.{schema}.silver_claims"
)

print(f"✓ Silver claims: {silver_claims.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: Revenue Summary (with margins, cumulative, MoM growth)

# COMMAND ----------

silver_revenue = spark.sql(f"""
WITH rev_cte AS (
  SELECT
    r.patient_id,
    r.month,
    r.upfront_recognized_usd,
    r.recurring_recognized_usd,
    r.gross_margin_pct,
    r.fulfillment_cost_usd,
    r.total_recognized_usd,
    r.margin_usd,
    r.channel,
    r.device_type,
    r._loaded_at,
    r._source,
    p.region,
    (r.total_recognized_usd * r.gross_margin_pct / 100) as margin_calculated_usd,
    CASE WHEN r.margin_usd > 0 THEN true ELSE false END as is_break_even,
    QUARTER(r.month) as quarter_num,
    YEAR(r.month) as year_num
  FROM {catalog}.{schema}.bronze_revenue_summary r
  JOIN {catalog}.{schema}.silver_patients p ON r.patient_id = p.patient_id
),
with_cumulative AS (
  SELECT
    *,
    SUM(total_recognized_usd) OVER (PARTITION BY patient_id ORDER BY month) as cumulative_usd,
    SUM(margin_usd) OVER (PARTITION BY patient_id ORDER BY month) as cumulative_margin_usd
  FROM rev_cte
),
with_mom_growth AS (
  SELECT
    *,
    LAG(total_recognized_usd) OVER (PARTITION BY patient_id ORDER BY month) as prev_month_total,
    CASE
      WHEN LAG(total_recognized_usd) OVER (PARTITION BY patient_id ORDER BY month) > 0
      THEN ROUND((total_recognized_usd - LAG(total_recognized_usd) OVER (PARTITION BY patient_id ORDER BY month))
                 / LAG(total_recognized_usd) OVER (PARTITION BY patient_id ORDER BY month) * 100, 2)
      ELSE 0
    END as moM_growth_pct
  FROM with_cumulative
)
SELECT
  patient_id,
  month,
  upfront_recognized_usd,
  recurring_recognized_usd,
  gross_margin_pct,
  fulfillment_cost_usd,
  total_recognized_usd,
  margin_usd,
  channel,
  device_type,
  region,
  margin_calculated_usd,
  is_break_even,
  quarter_num,
  year_num,
  cumulative_usd,
  cumulative_margin_usd,
  moM_growth_pct,
  _loaded_at,
  _source
FROM with_mom_growth
""")

silver_revenue.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
    f"{catalog}.{schema}.silver_revenue_summary"
)

print(f"✓ Silver revenue_summary: {silver_revenue.count()} rows")

print("\n✓ Silver layer complete!")
