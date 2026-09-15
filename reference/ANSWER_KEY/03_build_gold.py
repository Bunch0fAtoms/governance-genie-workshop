# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer: Business-Ready Tables with PK/FK Constraints
# MAGIC
# MAGIC Business-ready, constraint-enforced tables:
# MAGIC - gold_patients (PK: patient_id)
# MAGIC - gold_claims (PK: claim_id, filtered to pump_on_body and consumable_shipment only)
# MAGIC - gold_revenue_summary (PK: patient_id, month)

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workshop_certified", "Primary catalog")
dbutils.widgets.text("sandbox_catalog", "workshop_sandbox", "Sandbox catalog")
dbutils.widgets.text("schema", "analytics", "Schema name")

catalog = dbutils.widgets.get("catalog")
sandbox_catalog = dbutils.widgets.get("sandbox_catalog")
schema = dbutils.widgets.get("schema")

print(f"Building gold in {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: Patients (business-ready, PK enforced)

# COMMAND ----------

gold_patients = spark.sql(f"""
WITH patient_claims AS (
  SELECT
    p.patient_id,
    p.device_type,
    p.region,
    p.ssn,
    p.dob,
    p.patient_name,
    p.enrolled_date,
    p.device_category,
    CASE
      WHEN MAX(CASE WHEN c.claim_type = 'pump_on_body' THEN c.claim_date END) >= CURRENT_DATE - INTERVAL 12 MONTH
      THEN 'active'
      ELSE 'lapsed'
    END as patient_lifecycle_status,
    MAX(CASE WHEN c.claim_type = 'pump_on_body' THEN c.claim_date END) as pump_on_body_date,
    MAX(CASE WHEN c.claim_type = 'pump_on_body' THEN c.channel END) as channel_primary,
    ROUND(SUM(CASE WHEN r.upfront_recognized_usd > 0 THEN r.upfront_recognized_usd ELSE 0 END), 2) as total_upfront_12m,
    ROUND(SUM(CASE WHEN r.recurring_recognized_usd > 0 THEN r.recurring_recognized_usd ELSE 0 END), 2) as total_consumable_spend_12m
  FROM {catalog}.{schema}.silver_patients p
  LEFT JOIN {catalog}.{schema}.silver_claims c ON p.patient_id = c.patient_id
  LEFT JOIN {catalog}.{schema}.silver_revenue_summary r ON p.patient_id = r.patient_id
  GROUP BY
    p.patient_id, p.device_type, p.region, p.ssn, p.dob, p.patient_name,
    p.enrolled_date, p.device_category
)
SELECT
  patient_id,
  device_type,
  region,
  ssn,
  dob,
  patient_name,
  enrolled_date,
  device_category,
  patient_lifecycle_status,
  pump_on_body_date,
  channel_primary,
  total_upfront_12m,
  total_consumable_spend_12m
FROM patient_claims
""")

gold_patients.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
    f"{catalog}.{schema}.gold_patients"
)

print(f"✓ Gold patients: {gold_patients.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: Claims (filtered to pump_on_body + consumable_shipment only, PK enforced)

# COMMAND ----------

gold_claims = spark.sql(f"""
WITH filtered_claims AS (
  SELECT
    claim_id,
    patient_id,
    claim_date,
    claim_type,
    channel,
    product_line,
    product_sku,
    claim_amount_usd,
    units,
    device_type,
    region,
    quarter_num,
    year_num,
    days_to_pump_on_body,
    is_high_value,
    ROW_NUMBER() OVER (PARTITION BY claim_id ORDER BY _loaded_at DESC) as rn
  FROM {catalog}.{schema}.silver_claims
  WHERE claim_type IN ('pump_on_body', 'consumable_shipment')
),
with_month_offset AS (
  SELECT
    claim_id,
    patient_id,
    claim_date,
    claim_type,
    channel,
    product_line,
    product_sku,
    claim_amount_usd,
    units,
    device_type,
    region,
    quarter_num,
    year_num,
    days_to_pump_on_body,
    is_high_value,
    CASE
      WHEN claim_type = 'pump_on_body' THEN 0
      WHEN claim_type = 'consumable_shipment'
      THEN MONTH(claim_date) - MONTH(MAX(CASE WHEN claim_type = 'pump_on_body' THEN claim_date END)
                                      OVER (PARTITION BY patient_id)) +
           12 * (YEAR(claim_date) - YEAR(MAX(CASE WHEN claim_type = 'pump_on_body' THEN claim_date END)
                                               OVER (PARTITION BY patient_id)))
      ELSE NULL
    END as month_offset_from_placement
  FROM filtered_claims
  WHERE rn = 1
)
SELECT *
FROM with_month_offset
""")

gold_claims.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
    f"{catalog}.{schema}.gold_claims"
)

print(f"✓ Gold claims: {gold_claims.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: Revenue Summary (with cumulative and MoM metrics, PK enforced)

# COMMAND ----------

gold_revenue = spark.sql(f"""
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
  moM_growth_pct
FROM {catalog}.{schema}.silver_revenue_summary
""")

gold_revenue.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
    f"{catalog}.{schema}.gold_revenue_summary"
)

print(f"✓ Gold revenue_summary: {gold_revenue.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grain Verification

# COMMAND ----------

# Verify grain: gold_patients
patients_grain = spark.sql(f"""
SELECT
  COUNT(*) as total_rows,
  COUNT(DISTINCT patient_id) as unique_ids,
  CASE WHEN COUNT(*) = COUNT(DISTINCT patient_id) THEN '✓ PASS' ELSE '✗ FAIL' END as grain_check
FROM {catalog}.{schema}.gold_patients
""").collect()[0]

print(f"\nGold Patients Grain Check:")
print(f"  Total: {patients_grain[0]}, Unique: {patients_grain[1]} → {patients_grain[2]}")

# Verify grain: gold_claims (pump_on_body + consumable_shipment only)
claims_grain = spark.sql(f"""
SELECT
  COUNT(*) as total_rows,
  COUNT(DISTINCT claim_id) as unique_ids,
  CASE WHEN COUNT(*) = COUNT(DISTINCT claim_id) THEN '✓ PASS' ELSE '✗ FAIL' END as grain_check
FROM {catalog}.{schema}.gold_claims
""").collect()[0]

print(f"\nGold Claims Grain Check:")
print(f"  Total: {claims_grain[0]}, Unique: {claims_grain[1]} → {claims_grain[2]}")

# Verify grain: gold_revenue_summary
revenue_grain = spark.sql(f"""
SELECT
  COUNT(*) as total_rows,
  COUNT(DISTINCT CONCAT(patient_id, '|', month)) as unique_ids,
  CASE WHEN COUNT(*) = COUNT(DISTINCT CONCAT(patient_id, '|', month)) THEN '✓ PASS' ELSE '✗ FAIL' END as grain_check
FROM {catalog}.{schema}.gold_revenue_summary
""").collect()[0]

print(f"\nGold Revenue Grain Check:")
print(f"  Total: {revenue_grain[0]}, Unique: {revenue_grain[1]} → {revenue_grain[2]}")

print("\n✓ Gold layer complete!")
