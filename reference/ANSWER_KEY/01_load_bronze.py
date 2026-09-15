# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer: Raw Copy + Technical Metadata
# MAGIC
# MAGIC Copy foundation tables into bronze layer, adding:
# MAGIC - `_loaded_at` (current timestamp)
# MAGIC - `_source` ("foundation")
# MAGIC - `_has_phi` (true if ssn present)
# MAGIC - `_has_pii` (true if dob or name present)

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workshop_certified", "Primary catalog")
dbutils.widgets.text("sandbox_catalog", "workshop_sandbox", "Sandbox catalog")
dbutils.widgets.text("schema", "analytics", "Schema name")

catalog = dbutils.widgets.get("catalog")
sandbox_catalog = dbutils.widgets.get("sandbox_catalog")
schema = dbutils.widgets.get("schema")

print(f"Loading to bronze in {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: Patients

# COMMAND ----------

bronze_patients = spark.sql(f"""
SELECT
  patient_id,
  device_type,
  region,
  ssn,
  dob,
  patient_name,
  enrolled_date,
  current_timestamp() as _loaded_at,
  'foundation' as _source,
  true as _has_phi,
  true as _has_pii
FROM {catalog}.{schema}.patients
""")

bronze_patients.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
    f"{catalog}.{schema}.bronze_patients"
)

print(f"✓ Bronze patients: {bronze_patients.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: Claims

# COMMAND ----------

bronze_claims = spark.sql(f"""
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
  current_timestamp() as _loaded_at,
  'foundation' as _source,
  false as _has_phi,
  false as _has_pii
FROM {catalog}.{schema}.claims
""")

bronze_claims.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
    f"{catalog}.{schema}.bronze_claims"
)

print(f"✓ Bronze claims: {bronze_claims.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: Revenue Summary

# COMMAND ----------

bronze_revenue = spark.sql(f"""
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
  current_timestamp() as _loaded_at,
  'foundation' as _source,
  false as _has_phi,
  false as _has_pii
FROM {catalog}.{schema}.revenue_summary
""")

bronze_revenue.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
    f"{catalog}.{schema}.bronze_revenue_summary"
)

print(f"✓ Bronze revenue_summary: {bronze_revenue.count()} rows")

print("\n✓ Bronze layer complete!")
