# Databricks notebook source
# MAGIC %md
# MAGIC # Pre-built foundation: access control table and audit log (arc step 2)
# MAGIC
# MAGIC This runs as part of the GREEN foundation build, before the room arrives. It creates the
# MAGIC policy-as-code access surface so the room verifies it and extends the pattern, rather than
# MAGIC typing the boilerplate from scratch:
# MAGIC
# MAGIC - `workshop_governance.access_control`: the source of truth for grants, with an expiry column
# MAGIC   for auto-grant and auto-expire.
# MAGIC - `workshop_governance.access_control_audit`: the audit log for visibility.
# MAGIC - Seeded grants: a non-privileged analyst with 6 time-limited grants, a steward with 8
# MAGIC   permanent grants.
# MAGIC
# MAGIC The idempotent reconcile engine that turns these rows into real Unity Catalog grants is the
# MAGIC separate `apply_access_control` job; the room runs it on demand, and production runs it on a
# MAGIC schedule. This notebook only builds and seeds the table, so the foundation always runs green.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta

dbutils.widgets.text("catalog", "", "Primary catalog (required)")
dbutils.widgets.text("sandbox_catalog", "", "Sandbox catalog (defaults to catalog)")
dbutils.widgets.text("schema", "workshop_analytics", "Certified analytics schema")
dbutils.widgets.text("governance_schema", "workshop_governance", "Governance schema")

catalog = dbutils.widgets.get("catalog")
sandbox_catalog = dbutils.widgets.get("sandbox_catalog") or catalog
schema = dbutils.widgets.get("schema")
governance_schema = dbutils.widgets.get("governance_schema")

if not catalog:
    raise ValueError("catalog is required. Pass --var catalog=<your_catalog> or set the widget.")

gov = f"{catalog}.{governance_schema}"
print(f"Building access control in {gov}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Governance schema and tables

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gov} COMMENT 'Workshop governance: access control, audit, masks, filters'")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {gov}.access_control (
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
print("✓ Created access_control table")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {gov}.access_control_audit (
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
print("✓ Created access_control_audit table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Seed grants
# MAGIC
# MAGIC Example principals are `example.com` addresses, never real accounts. The analyst rows carry a
# MAGIC 14-day expiry (the sandbox-to-certified promotion story); the steward rows are permanent.

# COMMAND ----------

spark.sql(f"TRUNCATE TABLE {gov}.access_control")

now = datetime.now()
expires_14d = now + timedelta(days=14)

access_rows = [
    # Analyst: SELECT on the certified metric views (14-day sandbox expiry) = 6 active grants
    ("analyst@example.com", "user", "SCHEMA", f"{catalog}.{schema}", "USE_SCHEMA", expires_14d, "facilitator@databricks.com", now, "facilitator@databricks.com", True),
    ("analyst@example.com", "user", "VIEW", f"{catalog}.{schema}.mv_revenue_by_channel", "SELECT", expires_14d, "facilitator@databricks.com", now, "facilitator@databricks.com", True),
    ("analyst@example.com", "user", "VIEW", f"{catalog}.{schema}.mv_fulfillment_rate", "SELECT", expires_14d, "facilitator@databricks.com", now, "facilitator@databricks.com", True),
    ("analyst@example.com", "user", "VIEW", f"{catalog}.{schema}.mv_recognized_revenue", "SELECT", expires_14d, "facilitator@databricks.com", now, "facilitator@databricks.com", True),
    ("analyst@example.com", "user", "VIEW", f"{catalog}.{schema}.mv_recurring_revenue", "SELECT", expires_14d, "facilitator@databricks.com", now, "facilitator@databricks.com", True),
    ("analyst@example.com", "user", "VIEW", f"{catalog}.{schema}.mv_gross_margin", "SELECT", expires_14d, "facilitator@databricks.com", now, "facilitator@databricks.com", True),

    # Steward: permanent access to certified + governance = 8 active grants
    ("steward@example.com", "user", "SCHEMA", f"{catalog}.{schema}", "USE_SCHEMA", None, "facilitator@databricks.com", now, "facilitator@databricks.com", True),
    ("steward@example.com", "user", "TABLE", f"{catalog}.{schema}.gold_patients", "SELECT", None, "facilitator@databricks.com", now, "facilitator@databricks.com", True),
    ("steward@example.com", "user", "TABLE", f"{catalog}.{schema}.gold_claims", "SELECT", None, "facilitator@databricks.com", now, "facilitator@databricks.com", True),
    ("steward@example.com", "user", "TABLE", f"{catalog}.{schema}.gold_revenue_summary", "SELECT", None, "facilitator@databricks.com", now, "facilitator@databricks.com", True),
    ("steward@example.com", "user", "SCHEMA", f"{gov}", "USE_SCHEMA", None, "facilitator@databricks.com", now, "facilitator@databricks.com", True),
    ("steward@example.com", "user", "TABLE", f"{gov}.access_control", "SELECT", None, "facilitator@databricks.com", now, "facilitator@databricks.com", True),
    ("steward@example.com", "user", "TABLE", f"{gov}.access_control", "MODIFY", None, "facilitator@databricks.com", now, "facilitator@databricks.com", True),
    ("steward@example.com", "user", "TABLE", f"{gov}.access_control_audit", "SELECT", None, "facilitator@databricks.com", now, "facilitator@databricks.com", True),
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
        StructField("_active", BooleanType(), False),
    ])
)
access_df.write.mode("append").option("mergeSchema", "true").saveAsTable(f"{gov}.access_control")
print(f"✓ Seeded {len(access_rows)} access control rows (analyst 6, steward 8)")

# COMMAND ----------

audit_rows = [
    (now, "grant", "analyst@example.com", "user", "SCHEMA", f"{catalog}.{schema}", "USE_SCHEMA", "Foundation: initial sandbox setup", "foundation_job"),
    (now, "grant", "steward@example.com", "user", "SCHEMA", f"{gov}", "USE_SCHEMA", "Foundation: initial governance setup", "foundation_job"),
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
        StructField("executed_by", StringType(), False),
    ])
)
audit_df.write.mode("append").option("mergeSchema", "true").saveAsTable(f"{gov}.access_control_audit")
print("✓ Logged initial grants to the audit table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

active_grants = spark.sql(f"""
SELECT principal_id, COUNT(*) AS grant_count
FROM {gov}.access_control
WHERE _active = true AND (expires_at IS NULL OR expires_at > current_timestamp())
GROUP BY principal_id
ORDER BY principal_id
""").collect()

print("\n✓ Active grants (pre-built):")
for row in active_grants:
    print(f"  {row['principal_id']:30s}: {row['grant_count']:3d} grants")

print("\n✓ Access control table and audit log built green (analyst 6, steward 8).")
