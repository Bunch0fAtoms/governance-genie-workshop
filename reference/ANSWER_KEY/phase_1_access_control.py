# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 1: Speed of Business — Access Control Table (Auto-Expiring)
# MAGIC
# MAGIC Creates the governance infrastructure for Phase 1:
# MAGIC - access_control table (source of truth for grants)
# MAGIC - access_control_audit table (audit log for visibility)
# MAGIC - Seed with realistic sandbox and certified access rows

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta

dbutils.widgets.text("catalog", "workshop_certified", "Primary catalog")
dbutils.widgets.text("sandbox_catalog", "workshop_sandbox", "Sandbox catalog")
dbutils.widgets.text("schema", "analytics", "Schema name")

catalog = dbutils.widgets.get("catalog")
sandbox_catalog = dbutils.widgets.get("sandbox_catalog")
schema = dbutils.widgets.get("schema")

print(f"Phase 1: Setting up access control in {catalog}.workshop_governance")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Governance Schema and Tables

# COMMAND ----------

# Ensure governance schema exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.workshop_governance COMMENT 'Phase 1-3 Governance'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Access Control Table (Policy-as-Code)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.workshop_governance.access_control (
  principal_id STRING COMMENT 'User or group email',
  principal_type STRING COMMENT 'user or group',
  securable_type STRING COMMENT 'SCHEMA, TABLE, VIEW',
  securable_name STRING COMMENT 'Full securable name (catalog.schema.table)',
  privilege STRING COMMENT 'SELECT, MODIFY, USE_SCHEMA',
  expires_at TIMESTAMP COMMENT 'NULL = indefinite; auto-revoke after this',
  created_by STRING COMMENT 'Facilitator email',
  created_at TIMESTAMP COMMENT 'Grant creation time',
  granted_by STRING COMMENT 'Approver email',
  _active BOOLEAN COMMENT 'true if not yet expired'
)
USING DELTA
""")

print(f"✓ Created access_control table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Access Control Audit Table

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.workshop_governance.access_control_audit (
  audit_id BIGINT GENERATED ALWAYS AS IDENTITY,
  action_time TIMESTAMP NOT NULL,
  action STRING COMMENT 'grant, revoke, or expire',
  principal_id STRING NOT NULL,
  principal_type STRING NOT NULL,
  securable_type STRING NOT NULL,
  securable_name STRING NOT NULL,
  privilege STRING NOT NULL,
  reason STRING,
  executed_by STRING
)
USING DELTA
""")

print(f"✓ Created access_control_audit table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Seed Access Control Rows (Phase 1 + Phase 2 readiness)

# COMMAND ----------

# Clear existing rows to avoid duplication
spark.sql(f"TRUNCATE TABLE {catalog}.workshop_governance.access_control")

# Seed access control rows (realistic two-day sandbox + certified access)
now = datetime.now()
expires_14d = now + timedelta(days=14)
expires_never = None

access_rows = [
    # Analyst: SELECT on certified analytics (14-day sandbox expiry)
    ("analyst@example.com", "user", "SCHEMA", f"{catalog}.{schema}", "USE_SCHEMA", expires_14d, "facilitator@databricks.com", now, "facilitator@databricks.com", True),
    ("analyst@example.com", "user", "VIEW", f"{catalog}.{schema}.mv_revenue_by_channel", "SELECT", expires_14d, "facilitator@databricks.com", now, "facilitator@databricks.com", True),
    ("analyst@example.com", "user", "VIEW", f"{catalog}.{schema}.mv_fulfillment_rate", "SELECT", expires_14d, "facilitator@databricks.com", now, "facilitator@databricks.com", True),
    ("analyst@example.com", "user", "VIEW", f"{catalog}.{schema}.mv_recognized_revenue", "SELECT", expires_14d, "facilitator@databricks.com", now, "facilitator@databricks.com", True),
    ("analyst@example.com", "user", "VIEW", f"{catalog}.{schema}.mv_recurring_revenue", "SELECT", expires_14d, "facilitator@databricks.com", now, "facilitator@databricks.com", True),
    ("analyst@example.com", "user", "VIEW", f"{catalog}.{schema}.mv_gross_margin", "SELECT", expires_14d, "facilitator@databricks.com", now, "facilitator@databricks.com", True),

    # Steward: permanent access to certified + governance
    ("steward@example.com", "user", "SCHEMA", f"{catalog}.{schema}", "USE_SCHEMA", None, "facilitator@databricks.com", now, "facilitator@databricks.com", True),
    ("steward@example.com", "user", "TABLE", f"{catalog}.{schema}.gold_patients", "SELECT", None, "facilitator@databricks.com", now, "facilitator@databricks.com", True),
    ("steward@example.com", "user", "TABLE", f"{catalog}.{schema}.gold_claims", "SELECT", None, "facilitator@databricks.com", now, "facilitator@databricks.com", True),
    ("steward@example.com", "user", "TABLE", f"{catalog}.{schema}.gold_revenue_summary", "SELECT", None, "facilitator@databricks.com", now, "facilitator@databricks.com", True),
    ("steward@example.com", "user", "SCHEMA", f"{catalog}.workshop_governance", "USE_SCHEMA", None, "facilitator@databricks.com", now, "facilitator@databricks.com", True),
    ("steward@example.com", "user", "TABLE", f"{catalog}.workshop_governance.access_control", "SELECT", None, "facilitator@databricks.com", now, "facilitator@databricks.com", True),
    ("steward@example.com", "user", "TABLE", f"{catalog}.workshop_governance.access_control", "MODIFY", None, "facilitator@databricks.com", now, "facilitator@databricks.com", True),
    ("steward@example.com", "user", "TABLE", f"{catalog}.workshop_governance.access_control_audit", "SELECT", None, "facilitator@databricks.com", now, "facilitator@databricks.com", True),
]

access_df = spark.createDataFrame(
    access_rows,
    schema=StructType([
        StructField("principal_id", StringType(), False),
        StructField("principal_type", StringType(), False),
        StructField("securable_type", StringType(), False),
        StructField("securable_name", StringType(), False),
        StructField("privilege", StringType(), False),
        StructField("expires_at", TimestampType(), True),
        StructField("created_by", StringType(), False),
        StructField("created_at", TimestampType(), False),
        StructField("granted_by", StringType(), False),
        StructField("_active", BooleanType(), False)
    ])
)

access_df.write.mode("append").option("mergeSchema", "true").saveAsTable(
    f"{catalog}.workshop_governance.access_control"
)

print(f"✓ Seeded {len(access_rows)} access control rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Log Initial Seeding to Audit

# COMMAND ----------

audit_rows = [
    (now, "grant", "analyst@example.com", "user", "SCHEMA", f"{catalog}.{schema}", "USE_SCHEMA", "Phase 1: Initial sandbox setup", "foundation_job"),
    (now, "grant", "steward@example.com", "user", "SCHEMA", f"{catalog}.workshop_governance", "USE_SCHEMA", "Phase 1: Initial governance setup", "foundation_job"),
]

audit_df = spark.createDataFrame(
    audit_rows,
    schema=StructType([
        StructField("action_time", TimestampType(), False),
        StructField("action", StringType(), False),
        StructField("principal_id", StringType(), False),
        StructField("principal_type", StringType(), False),
        StructField("securable_type", StringType(), False),
        StructField("securable_name", StringType(), False),
        StructField("privilege", StringType(), False),
        StructField("reason", StringType(), False),
        StructField("executed_by", StringType(), False)
    ])
)

audit_df.write.mode("append").option("mergeSchema", "true").saveAsTable(
    f"{catalog}.workshop_governance.access_control_audit"
)

print(f"✓ Logged initial grants to audit table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

active_grants = spark.sql(f"""
SELECT principal_id, COUNT(*) as grant_count
FROM {catalog}.workshop_governance.access_control
WHERE _active = true AND (expires_at IS NULL OR expires_at > current_timestamp())
GROUP BY principal_id
ORDER BY principal_id
""").collect()

print("\n✓ Active grants:")
for row in active_grants:
    print(f"  {row[0]:30s}: {row[1]:3d} grants")

print("\n✓ Phase 1 access control table created and seeded!")
