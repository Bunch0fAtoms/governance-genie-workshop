# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog Metric Views: the governed semantic layer for Genie
# MAGIC
# MAGIC Builds the five commercial metric views (plus one Healthcare Provider (HCP) view) as real
# MAGIC Unity Catalog **Metric Views** (`CREATE ... WITH METRICS LANGUAGE YAML`), not plain SQL views.
# MAGIC Each declares certified measures and dimensions over the gold tables, so the definitions live
# MAGIC once, governed, and both Genie and Structured Query Language (SQL) query them with `MEASURE()`.
# MAGIC
# MAGIC 1. mv_revenue_by_channel: revenue by channel, product, quarter
# MAGIC 2. mv_fulfillment_rate: Patient Services Agreement (PSA) to Pump on Body cycle time (oracle query 1)
# MAGIC 3. mv_recognized_revenue: per-patient, per-month revenue recognition
# MAGIC 4. mv_recurring_revenue: consumable revenue cohorts by device and channel
# MAGIC 5. mv_gross_margin: margin by channel and device (oracle query 2)
# MAGIC 6. mv_hcp_referral_performance: HCP referral and conversion metrics
# MAGIC
# MAGIC All sources are GOLD materialized views (certified data). Requires Databricks Runtime 17.2+
# MAGIC (metric-view YAML version 1.1). Query with `MEASURE(\`Measure Name\`)` and `GROUP BY ALL`.

# COMMAND ----------

dbutils.widgets.text("catalog", "workshop_certified", "Primary catalog")
dbutils.widgets.text("schema", "workshop_analytics", "Schema name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
base = f"{catalog}.{schema}"

print(f"Building Unity Catalog metric views in {base}")

# COMMAND ----------

def create_metric_view(name: str, yaml_body: str):
    """Drop any existing view of this name (plain or metric), then create the metric view.
    CREATE OR REPLACE cannot swap a plain view for a metric view, so drop first for idempotency."""
    fq = f"{base}.{name}"
    spark.sql(f"DROP VIEW IF EXISTS {fq}")
    spark.sql(f"CREATE VIEW {fq}\nWITH METRICS\nLANGUAGE YAML\nAS $$\n{yaml_body}\n$$")
    print(f"✓ Created metric view: {name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. mv_revenue_by_channel

# COMMAND ----------

create_metric_view("mv_revenue_by_channel", f"""
version: 1.1
source: {base}.gold_revenue_summary
comment: "Recognized revenue by channel, device, and quarter (certified)."
filter: is_break_even = true
dimensions:
  - name: Channel
    expr: channel
  - name: Device Type
    expr: device_type
  - name: Quarter
    expr: quarter_num
  - name: Year
    expr: year_num
measures:
  - name: Patient Count
    expr: COUNT(DISTINCT patient_id)
  - name: Total Upfront USD
    expr: SUM(upfront_recognized_usd)
  - name: Total Recurring USD
    expr: SUM(recurring_recognized_usd)
  - name: Total Revenue USD
    expr: SUM(total_recognized_usd)
  - name: Total Margin USD
    expr: SUM(margin_usd)
  - name: Avg Margin Pct
    expr: AVG(gross_margin_pct)
""".strip())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. mv_fulfillment_rate (oracle query 1: cycle time)
# MAGIC
# MAGIC Filter Claim Type to `pump_on_body` and slice by Channel and Device Type for the cycle-time answer.

# COMMAND ----------

create_metric_view("mv_fulfillment_rate", f"""
version: 1.1
source: {base}.gold_claims
comment: "PSA-to-Pump-on-Body cycle time and claim volume by channel (certified)."
filter: claim_type IN ('pump_on_body', 'consumable_shipment')
dimensions:
  - name: Channel
    expr: channel
  - name: Device Type
    expr: device_type
  - name: Claim Type
    expr: claim_type
  - name: Region
    expr: region
  - name: Quarter
    expr: quarter_num
  - name: Year
    expr: year_num
  - name: Claim Date
    expr: claim_date
measures:
  - name: Avg Cycle Time Days
    expr: AVG(days_to_pump_on_body)
  - name: Patient Count
    expr: COUNT(DISTINCT patient_id)
  - name: Claim Count
    expr: COUNT(1)
""".strip())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. mv_recognized_revenue

# COMMAND ----------

create_metric_view("mv_recognized_revenue", f"""
version: 1.1
source: {base}.gold_revenue_summary
comment: "Per-patient, per-month recognized revenue (upfront and recurring), certified."
dimensions:
  - name: Patient Id
    expr: patient_id
  - name: Month
    expr: month
  - name: Channel
    expr: channel
  - name: Device Type
    expr: device_type
  - name: Region
    expr: region
  - name: Quarter
    expr: quarter_num
  - name: Year
    expr: year_num
  - name: Is Break Even
    expr: is_break_even
measures:
  - name: Upfront USD
    expr: SUM(upfront_recognized_usd)
  - name: Recurring USD
    expr: SUM(recurring_recognized_usd)
  - name: Total Revenue USD
    expr: SUM(total_recognized_usd)
  - name: Margin USD
    expr: SUM(margin_usd)
  - name: Avg Margin Pct
    expr: AVG(gross_margin_pct)
""".strip())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. mv_recurring_revenue
# MAGIC
# MAGIC Consumable revenue cohorts by device and channel, joined to patient lifecycle status.

# COMMAND ----------

create_metric_view("mv_recurring_revenue", f"""
version: 1.1
source: {base}.gold_revenue_summary
comment: "Consumable (recurring) revenue cohorts by channel, device, and lifecycle status."
filter: recurring_recognized_usd > 0
joins:
  - name: patient
    source: {base}.gold_patients
    on: source.patient_id = patient.patient_id
dimensions:
  - name: Channel
    expr: channel
  - name: Device Type
    expr: device_type
  - name: Lifecycle Status
    expr: patient.patient_lifecycle_status
measures:
  - name: Patient Cohort Size
    expr: COUNT(DISTINCT patient_id)
  - name: Total Consumable Revenue 12m
    expr: SUM(recurring_recognized_usd)
  - name: Avg Monthly Consumable USD
    expr: AVG(recurring_recognized_usd)
  - name: Avg Margin Pct
    expr: AVG(gross_margin_pct)
""".strip())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. mv_gross_margin (oracle query 2: margin by channel)

# COMMAND ----------

create_metric_view("mv_gross_margin", f"""
version: 1.1
source: {base}.gold_revenue_summary
comment: "Gross margin percent and dollars by channel, device, and region (certified)."
filter: is_break_even = true
dimensions:
  - name: Channel
    expr: channel
  - name: Device Type
    expr: device_type
  - name: Region
    expr: region
  - name: Month
    expr: month
  - name: Quarter
    expr: quarter_num
  - name: Year
    expr: year_num
measures:
  - name: Patient Count
    expr: COUNT(DISTINCT patient_id)
  - name: Revenue USD
    expr: SUM(total_recognized_usd)
  - name: Margin USD
    expr: SUM(margin_usd)
  - name: Fulfillment Cost USD
    expr: SUM(fulfillment_cost_usd)
  - name: Gross Margin Pct
    expr: AVG(gross_margin_pct)
""".strip())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. mv_hcp_referral_performance (commercial HCP arc)

# COMMAND ----------

create_metric_view("mv_hcp_referral_performance", f"""
version: 1.1
source: {base}.gold_hcp
comment: "Healthcare Provider referral and conversion metrics for commercial analysis (certified)."
dimensions:
  - name: HCP Id
    expr: hcp_id
  - name: HCP Name
    expr: hcp_name
  - name: Facility
    expr: facility
  - name: Specialty
    expr: specialty
  - name: Role Type
    expr: role_type
  - name: Region
    expr: region
  - name: Segment Priority
    expr: segment_priority
  - name: Primary Device Referred
    expr: primary_device_referred
measures:
  - name: HCP Count
    expr: COUNT(DISTINCT hcp_id)
  - name: Patients Referred
    expr: SUM(patients_referred)
  - name: Active Placements
    expr: SUM(active_placements)
  - name: Avg Placement Success Rate
    expr: AVG(placement_success_rate)
  - name: Avg Retention Rate
    expr: AVG(retention_rate)
  - name: Total NBRx 12m
    expr: SUM(nbrx_12m)
  - name: Total TRx 12m
    expr: SUM(trx_12m)
""".strip())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification: query the two oracle metric views with MEASURE()

# COMMAND ----------

# Oracle 1: cycle time by channel and device, Q1 2026, Pump on Body. Expect DTC fastest (~26-30 days).
print("Oracle 1 - Avg cycle time by channel/device, Q1 2026 (Pump on Body):")
q1 = spark.sql(f"""
SELECT Channel, `Device Type`, ROUND(MEASURE(`Avg Cycle Time Days`)) AS avg_days
FROM {base}.mv_fulfillment_rate
WHERE `Claim Type` = 'pump_on_body' AND Quarter = 1 AND Year = 2026
GROUP BY ALL
ORDER BY avg_days
""")
q1.show(truncate=False)

# Oracle 2: gross margin by channel. Expect DTC 50 > Pharmacy 45 > DME 40.
print("Oracle 2 - Gross margin percent by channel:")
q2 = spark.sql(f"""
SELECT Channel, ROUND(MEASURE(`Gross Margin Pct`), 1) AS gross_margin_pct
FROM {base}.mv_gross_margin
GROUP BY ALL
ORDER BY gross_margin_pct DESC
""")
q2.show(truncate=False)

# COMMAND ----------

# Confirm all six metric views exist and are typed as METRIC_VIEW.
views = ["mv_revenue_by_channel","mv_fulfillment_rate","mv_recognized_revenue",
         "mv_recurring_revenue","mv_gross_margin","mv_hcp_referral_performance"]
for v in views:
    kind = spark.sql(f"DESCRIBE TABLE EXTENDED {base}.{v}") \
        .filter("col_name = 'Type'").collect()
    label = kind[0]["data_type"] if kind else "?"
    print(f"✓ {v:32s}: {label}")

print("\n✓ All six Unity Catalog metric views created.")
