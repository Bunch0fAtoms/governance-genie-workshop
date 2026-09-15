# Databricks notebook source
# MAGIC %md
# MAGIC # Foundation Build: Synthetic Patients, Claims, Revenue, HCPs (Tandem Channel-Shift)
# MAGIC
# MAGIC This notebook generates synthetic data for the Tandem three-phase governance + Genie + HCP commercial workshop.
# MAGIC All data is 100% synthetic for capability demonstration.
# MAGIC
# MAGIC **Generates:**
# MAGIC - ~120 HCPs (healthcare providers with specialty, region, segment priority, referral metrics)
# MAGIC - 300 patients (with PHI: ssn, dob, patient_name; each referred by an HCP)
# MAGIC - 4,200 raw claims (1 PSA intake, 1 Pump on Body, 12 consumable shipments per patient); gold_claims keeps 3,900
# MAGIC - 3,600 revenue_summary rows (one per patient-month post-placement)
# MAGIC
# MAGIC **Oracle relationships (planted, validated by query):**
# MAGIC - Cycle time: DTC ~30 days < Pharmacy ~40 < DME ~60
# MAGIC - Gross margin: DTC ~50% > Pharmacy ~45% > DME ~40%
# MAGIC - Q1 well-populated for cycle-time query; Q2-Q3 for margin query
# MAGIC - HCP referral patterns: patients distributed across HCPs by region, with role (prescriber/influencer) and device preference

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "workshop_certified", "Primary certified catalog")
dbutils.widgets.text("sandbox_catalog", "workshop_sandbox", "Sandbox catalog for Phase 1")
dbutils.widgets.text("schema", "analytics", "Schema for gold tables and views")

catalog = dbutils.widgets.get("catalog")
sandbox_catalog = dbutils.widgets.get("sandbox_catalog")
schema = dbutils.widgets.get("schema")

print(f"Catalog (certified): {catalog}")
print(f"Catalog (sandbox):   {sandbox_catalog}")
print(f"Schema:             {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Catalogs and Schemas

# COMMAND ----------

# NOTE: Catalogs are pre-provisioned on this workspace. Some workspaces do not allow CREATE CATALOG on
# the shared metastore, so we do NOT create catalogs here; we create schemas inside the
# existing catalog(s) passed in via parameters. On Tandem's own Azure metastore, the two
# catalogs (workshop_certified / workshop_sandbox) can be created up front and passed in instead.

# Create the certified analytics + governance schemas in the certified catalog
for s in [schema, "workshop_governance"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{s}")

# Create the uncertified sandbox schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {sandbox_catalog}.workshop_sandbox COMMENT 'Phase 1 sandbox (uncertified)'")

print("Catalogs and schemas created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Migration guard: clear any prior regular medallion tables
# MAGIC
# MAGIC The bronze, silver, and gold layers are now built as materialized views by the Spark
# MAGIC Declarative Pipeline (pipeline/*.sql). If an earlier imperative build left them as regular
# MAGIC managed tables, the pipeline cannot create a materialized view of the same name, so we drop
# MAGIC those regular tables here. On a fresh workspace none exist (no-op); after the pipeline has run
# MAGIC once they are materialized views owned by the pipeline, and the guarded DROP TABLE skips them.

# COMMAND ----------

# Drop only PRIOR regular tables named like the pipeline's medallion outputs. DROP TABLE on a
# materialized view raises (EXPECT_TABLE_NOT_VIEW), which we swallow so pipeline-owned MVs are left
# in place. This keeps a fresh deploy clean and a redeploy from the old notebook build self-healing.
# Includes HCP tables (bronze, silver, gold) as part of the medallion.
_medallion_objects = [
    "bronze_patients", "bronze_claims", "bronze_revenue_summary", "bronze_hcps",
    "silver_patients", "silver_claims", "silver_revenue_summary", "silver_hcps",
    "gold_patients", "gold_claims", "gold_revenue_summary", "gold_hcp",
]
for _obj in _medallion_objects:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.{_obj}")
        print(f"✓ Cleared prior regular table (if any): {_obj}")
    except Exception as e:
        print(f"• Skipped {_obj} (already a pipeline materialized view): {str(e)[:80]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Synthetic Data Generation
# MAGIC
# MAGIC **Oracle relationships (hard-coded):**
# MAGIC - PSA-to-Pump-on-Body cycle time: DME [45-75 days] / Pharmacy [30-60] / DTC [15-45]
# MAGIC - Gross margin: DME 40% / Pharmacy 45% / DTC 50%
# MAGIC - Device split: Mobi 60% / t:slim X2 40%
# MAGIC - Channel split: DME 40% / Pharmacy 30% / DTC 30%
# MAGIC - Q1 and Q2-Q3 well-populated for oracle queries

# COMMAND ----------

# Set seed for reproducibility
random.seed(42)
spark_seed = 42

# COMMAND ----------

# MAGIC %md
# MAGIC ### 0. Generate HCPs (Healthcare Providers, ~120 rows)
# MAGIC
# MAGIC Synthetic HCP pool for commercial referral and targeting (Phase 4 HCP Arc).
# MAGIC One row per HCP; patients will reference these via referring_hcp_id.

# COMMAND ----------

num_hcps = 120
hcp_specialties = ["Endocrinologist", "Diabetes Specialist", "PCP", "Nurse Educator", "PA-NP"]
hcp_role_types = ["prescriber", "influencer"]
hcp_regions = ["US West", "US South", "US East"]
hcp_segment_priorities = ["High", "Medium", "Low"]
hcp_devices = ["t:slim X2", "Mobi", "Competitor"]

hcp_data = []
hcp_first_names = ["Sarah", "Michael", "Jennifer", "David", "Emily", "Robert", "Jessica", "James",
                   "Lisa", "William", "Amanda", "Richard", "Christina", "Joseph", "Michelle"]
hcp_last_names = ["Anderson", "Taylor", "Martinez", "Brown", "Davis", "Miller", "Wilson", "Moore",
                  "Jackson", "White", "Harris", "Martin", "Thompson", "Garcia", "Robinson"]

for i in range(num_hcps):
    hcp_id = f"HCP-{i:06d}"
    first_name = random.choice(hcp_first_names)
    last_name = random.choice(hcp_last_names)
    hcp_name = f"{first_name} {last_name}"
    facility = random.choice([
        "Regional Diabetes Center", "County Medical Center", "Endocrinology Associates",
        "Community Health Clinic", "University Hospital", "Specialty Care Group"
    ])
    specialty = random.choice(hcp_specialties)
    role_type = random.choices(hcp_role_types, weights=[70, 30])[0]  # 70% prescriber, 30% influencer
    region = random.choice(hcp_regions)
    segment_priority = random.choices(hcp_segment_priorities, weights=[35, 35, 30])[0]  # 35% High/Med, 30% Low
    primary_device_referred = random.choices(hcp_devices, weights=[40, 40, 20])[0]  # 40% each X2/Mobi, 20% Competitor
    nbrx_12m = random.randint(5, 50)  # New starts per year
    trx_12m = nbrx_12m + random.randint(20, 100)  # Total prescriptions (must be >= nbrx_12m)

    hcp_data.append((
        hcp_id, hcp_name, facility, specialty, role_type, region, segment_priority,
        primary_device_referred, nbrx_12m, trx_12m
    ))

hcp_df = spark.createDataFrame(
    hcp_data,
    schema=StructType([
        StructField("hcp_id", StringType(), False),
        StructField("hcp_name", StringType(), False),
        StructField("facility", StringType(), False),
        StructField("specialty", StringType(), False),
        StructField("role_type", StringType(), False),
        StructField("region", StringType(), False),
        StructField("segment_priority", StringType(), False),
        StructField("primary_device_referred", StringType(), False),
        StructField("nbrx_12m", IntegerType(), False),
        StructField("trx_12m", IntegerType(), False)
    ])
)

hcp_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
    f"{catalog}.{schema}.hcps"
)

print(f"✓ Created HCPs: {hcp_df.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Generate PATIENTS (300 rows, grain: patient_id)

# COMMAND ----------

num_patients = 300
devices = ["Mobi", "t:slim X2"]
regions = ["US East", "US West", "US South"]
channels = ["DME", "Pharmacy", "DTC"]

# Generate patient data
patients_data = []
base_date = datetime(2024, 1, 1)

# Collect HCPs for referral assignment
hcps_list = hcp_df.collect()

for i in range(num_patients):
    patient_id = f"PAT-{i:06d}"
    device_type = random.choices(devices, weights=[60, 40])[0]
    region = random.choice(regions)

    # Generate realistic PHI
    ssn = f"{random.randint(100, 999)}-{random.randint(10, 99)}-{random.randint(1000, 9999)}"
    years_ago = random.randint(0, 2)
    days_ago = random.randint(0, 365)
    dob = base_date - timedelta(days=365*random.randint(18, 75))
    patient_name = random.choice([
        "John Smith", "Mary Johnson", "James Williams", "Patricia Brown",
        "Michael Davis", "Jennifer Miller", "David Wilson", "Linda Moore",
        "Richard Taylor", "Barbara Anderson", "Joseph Thomas", "Susan Jackson",
        "Charles White", "Sarah Harris", "Mark Martin", "Karen Thompson",
        "Donald Lee", "Nancy Garcia", "Steven Robinson", "Lisa Clark",
        "Paul Rodriguez", "Betty Lewis", "Andrew Walker", "Margaret Hall",
        "Joshua Young", "Dorothy Hernandez", "Kevin King", "Dolores Wright",
        "Brian Lopez", "Diane Sanchez", "George Morris", "Joyce Rogers"
    ])

    enrolled_date = base_date + timedelta(days=random.randint(0, 730))

    # Assign a referring HCP (prefer HCPs in the same region, but allow cross-region)
    region_hcps = [h for h in hcps_list if h.region == region]
    if region_hcps and random.random() < 0.75:  # 75% chance to use an HCP from the same region
        referring_hcp = random.choice(region_hcps)
    else:
        referring_hcp = random.choice(hcps_list)
    referring_hcp_id = referring_hcp.hcp_id

    patients_data.append((
        patient_id, device_type, region, ssn, dob, patient_name, enrolled_date, referring_hcp_id
    ))

patients_df = spark.createDataFrame(
    patients_data,
    schema=StructType([
        StructField("patient_id", StringType(), False),
        StructField("device_type", StringType(), False),
        StructField("region", StringType(), False),
        StructField("ssn", StringType(), False),
        StructField("dob", DateType(), False),
        StructField("patient_name", StringType(), False),
        StructField("enrolled_date", DateType(), False),
        StructField("referring_hcp_id", StringType(), False)
    ])
)

patients_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
    f"{catalog}.{schema}.patients"
)

print(f"✓ Created patients: {patients_df.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Generate CLAIMS (4,200 raw rows; 3,900 land in gold_claims; grain: claim_id)
# MAGIC
# MAGIC One claim per patient per event type:
# MAGIC - PSA intake (1 per patient) at enrollment + [0-30] random days
# MAGIC - Pump on Body (1 per patient) at PSA + [cycle_time based on channel]
# MAGIC - Consumable shipments (12 per patient) monthly for 12 months post-placement
# MAGIC
# MAGIC **Oracle: Cycle times planted here (lines ~70-100)**

# COMMAND ----------

# Collect patients for joining in Python
patients_list = patients_df.collect()

claims_data = []
claim_counter = 0

# Channel-to-cycle-time mapping (oracle relationship)
cycle_time_ranges = {
    "DME": (45, 75),
    "Pharmacy": (30, 60),
    "DTC": (15, 45)
}

# Channel-to-margin mapping (oracle relationship for query 2)
margin_map = {
    "DME": 0.40,
    "Pharmacy": 0.45,
    "DTC": 0.50
}

for patient in patients_list:
    patient_id = patient.patient_id
    device_type = patient.device_type
    region = patient.region
    enrolled_date = patient.enrolled_date

    # Assign channel based on random choice (40% DME, 30% Pharmacy, 30% DTC)
    channel = random.choices(channels, weights=[40, 30, 30])[0]

    # PSA intake claim (event 1)
    psa_date = enrolled_date + timedelta(days=random.randint(0, 30))
    # Ensure PSA is in Q1 2026 for oracle query (roughly)
    if random.random() < 0.7:  # 70% chance to force into Q1 for better oracle data
        psa_date = datetime(2026, 1, 1) + timedelta(days=random.randint(0, 89))
    # enrolled_date returns from Spark as a datetime.date, so the first branch yields a
    # date and the override yields a datetime. Normalize to datetime so .date() is always valid.
    if not isinstance(psa_date, datetime):
        psa_date = datetime(psa_date.year, psa_date.month, psa_date.day)

    claim_id_psa = f"CLM-{claim_counter:07d}"
    claim_counter += 1

    claims_data.append((
        claim_id_psa, patient_id, psa_date.date(), "psa_intake",
        channel, "Durables", "psa", 0.0, 1
    ))

    # Pump on Body claim (event 2) — cycle time planted here (oracle query 1)
    cycle_min, cycle_max = cycle_time_ranges[channel]
    cycle_days = random.randint(cycle_min, cycle_max)
    pump_on_body_date = psa_date + timedelta(days=cycle_days)

    claim_id_pump = f"CLM-{claim_counter:07d}"
    claim_counter += 1

    # Upfront revenue for durable (device-specific, ~$4000-5500)
    upfront_amount = 4500.0 if device_type == "Mobi" else 5400.0

    claims_data.append((
        claim_id_pump, patient_id, pump_on_body_date.date(), "pump_on_body",
        channel, "Durables", device_type, upfront_amount, 1
    ))

    # Consumable shipments (12 monthly claims, starting month after pump on body)
    for month_offset in range(1, 13):
        shipment_date = pump_on_body_date.replace(day=1) + timedelta(days=32)  # Rough month advance
        shipment_date = shipment_date.replace(day=1)  # Snap to 1st of month
        # Adjust for exact month
        target_month = pump_on_body_date.month + month_offset
        target_year = pump_on_body_date.year
        if target_month > 12:
            target_month -= 12
            target_year += 1
        shipment_date = datetime(target_year, target_month, 1)

        claim_id_consumable = f"CLM-{claim_counter:07d}"
        claim_counter += 1

        # Consumable revenue (~$100-150 per shipment)
        consumable_amount = random.uniform(100, 150)

        claims_data.append((
            claim_id_consumable, patient_id, shipment_date.date(), "consumable_shipment",
            channel, "Consumables", "infusion_set", consumable_amount, 1
        ))

claims_df = spark.createDataFrame(
    claims_data,
    schema=StructType([
        StructField("claim_id", StringType(), False),
        StructField("patient_id", StringType(), False),
        StructField("claim_date", DateType(), False),
        StructField("claim_type", StringType(), False),
        StructField("channel", StringType(), False),
        StructField("product_line", StringType(), False),
        StructField("product_sku", StringType(), False),
        StructField("claim_amount_usd", DoubleType(), False),
        StructField("units", IntegerType(), False)
    ])
)

claims_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
    f"{catalog}.{schema}.claims"
)

print(f"✓ Created claims: {claims_df.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Generate REVENUE_SUMMARY (3,600 rows, grain: patient_id, month)
# MAGIC
# MAGIC One row per patient-month post-Pump-on-Body placement.
# MAGIC Recognizes upfront revenue in placement month; recurring revenue monthly thereafter.
# MAGIC **Oracle: Gross margins by channel planted here (lines ~150-180)**

# COMMAND ----------

revenue_data = []

# Re-join to get channel for each patient (regenerate for consistency)
for i, patient in enumerate(patients_list):
    patient_id = patient.patient_id
    region = patient.region
    device_type = patient.device_type

    # Retrieve corresponding claims to find pump-on-body date and channel
    patient_claims = [c for c in claims_data if c[1] == patient_id]
    pump_on_body_claim = [c for c in patient_claims if c[3] == "pump_on_body"][0]
    pump_on_body_date = pump_on_body_claim[2]
    channel = pump_on_body_claim[4]

    # For each of 12 months post-placement, create a revenue_summary row
    for month_offset in range(0, 12):
        target_month = pump_on_body_date.month + month_offset
        target_year = pump_on_body_date.year
        if target_month > 12:
            target_month -= 12
            target_year += 1

        month_date = datetime(target_year, target_month, 1).date()

        # Upfront revenue in placement month only
        if month_offset == 0:
            upfront_recognized = 4500.0 if device_type == "Mobi" else 5400.0
        else:
            upfront_recognized = 0.0

        # Recurring revenue (consumables) every month
        recurring_recognized = random.uniform(100, 150)

        # Gross margin by channel (oracle relationship for query 2)
        margin_pct = margin_map[channel]
        gross_margin_pct = margin_pct * 100

        # Total recognized
        total_recognized = upfront_recognized + recurring_recognized
        margin_usd = total_recognized * margin_pct

        revenue_data.append((
            patient_id, month_date, upfront_recognized, recurring_recognized,
            gross_margin_pct, 50.0 if channel == "DTC" else (40.0 if channel == "DME" else 45.0),
            total_recognized, margin_usd, channel, device_type
        ))

revenue_df = spark.createDataFrame(
    revenue_data,
    schema=StructType([
        StructField("patient_id", StringType(), False),
        StructField("month", DateType(), False),
        StructField("upfront_recognized_usd", DoubleType(), False),
        StructField("recurring_recognized_usd", DoubleType(), False),
        StructField("gross_margin_pct", DoubleType(), False),
        StructField("fulfillment_cost_usd", DoubleType(), False),
        StructField("total_recognized_usd", DoubleType(), False),
        StructField("margin_usd", DoubleType(), False),
        StructField("channel", StringType(), False),
        StructField("device_type", StringType(), False)
    ])
)

revenue_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
    f"{catalog}.{schema}.revenue_summary"
)

print(f"✓ Created revenue_summary: {revenue_df.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification: Grain, HCPs, and Oracle

# COMMAND ----------

# Grain check: HCPs
hcp_count = spark.sql(f"SELECT COUNT(*) FROM {catalog}.{schema}.hcps").collect()[0][0]
hcp_unique = spark.sql(f"SELECT COUNT(DISTINCT hcp_id) FROM {catalog}.{schema}.hcps").collect()[0][0]
print(f"HCPs: {hcp_count} rows, {hcp_unique} unique hcp_id → grain OK: {hcp_count == hcp_unique}")

# HCP segment priority breakdown
hcp_segments = spark.sql(f"SELECT segment_priority, COUNT(*) as count FROM {catalog}.{schema}.hcps GROUP BY segment_priority ORDER BY segment_priority").collect()
print("  HCP segment distribution:")
for row in hcp_segments:
    print(f"    {row[0]:10s}: {row[1]:3d}")

# HCP specialty breakdown
hcp_specialties = spark.sql(f"SELECT specialty, COUNT(*) as count FROM {catalog}.{schema}.hcps GROUP BY specialty ORDER BY specialty").collect()
print("  HCP specialty distribution:")
for row in hcp_specialties:
    print(f"    {row[0]:20s}: {row[1]:3d}")

# HCP role type breakdown
hcp_roles = spark.sql(f"SELECT role_type, COUNT(*) as count FROM {catalog}.{schema}.hcps GROUP BY role_type ORDER BY role_type").collect()
print("  HCP role type distribution:")
for row in hcp_roles:
    print(f"    {row[0]:15s}: {row[1]:3d}")

# Grain check: patients
patients_count = spark.sql(f"SELECT COUNT(*) FROM {catalog}.{schema}.patients").collect()[0][0]
patients_unique = spark.sql(f"SELECT COUNT(DISTINCT patient_id) FROM {catalog}.{schema}.patients").collect()[0][0]
print(f"\nPatients: {patients_count} rows, {patients_unique} unique patient_id → grain OK: {patients_count == patients_unique}")

# Verify all patients have a referring_hcp_id
patients_with_hcp = spark.sql(f"SELECT COUNT(*) FROM {catalog}.{schema}.patients WHERE referring_hcp_id IS NOT NULL").collect()[0][0]
print(f"  Patients with referring_hcp_id: {patients_with_hcp}/{patients_count} ✓")

# Grain check: claims (one row per claim event)
claims_count = spark.sql(f"SELECT COUNT(*) FROM {catalog}.{schema}.claims").collect()[0][0]
claims_unique = spark.sql(f"SELECT COUNT(DISTINCT claim_id) FROM {catalog}.{schema}.claims").collect()[0][0]
print(f"\nClaims: {claims_count} rows, {claims_unique} unique claim_id → grain OK: {claims_count == claims_unique}")

# Grain check: revenue_summary
revenue_count = spark.sql(f"SELECT COUNT(*) FROM {catalog}.{schema}.revenue_summary").collect()[0][0]
revenue_unique = spark.sql(f"SELECT COUNT(DISTINCT patient_id, month) FROM {catalog}.{schema}.revenue_summary").collect()[0][0]
print(f"Revenue: {revenue_count} rows, {revenue_unique} unique (patient_id, month) → grain OK: {revenue_count == revenue_unique}")

# Oracle: Cycle time by channel (Q1 2026)
cycle_check = spark.sql(f"""
SELECT channel, ROUND(AVG(claim_date_diff)) as avg_days, COUNT(*) as count
FROM (
  SELECT
    p.patient_id,
    c_psa.channel,
    DATEDIFF(c_pump.claim_date, c_psa.claim_date) as claim_date_diff
  FROM {catalog}.{schema}.patients p
  JOIN {catalog}.{schema}.claims c_psa ON p.patient_id = c_psa.patient_id AND c_psa.claim_type = 'psa_intake'
  JOIN {catalog}.{schema}.claims c_pump ON p.patient_id = c_pump.patient_id AND c_pump.claim_type = 'pump_on_body'
)
GROUP BY channel
ORDER BY avg_days
""").collect()

print("\n✓ ORACLE 1: Cycle Time by Channel")
for row in cycle_check:
    print(f"  {row[0]:12s}: ~{row[1]:.0f} days (n={int(row[2])})")

# Oracle: Gross margin by channel
margin_check = spark.sql(f"""
SELECT channel, ROUND(AVG(gross_margin_pct), 2) as avg_margin, COUNT(*) as count
FROM {catalog}.{schema}.revenue_summary
GROUP BY channel
ORDER BY avg_margin DESC
""").collect()

print("\n✓ ORACLE 2: Gross Margin by Channel")
for row in margin_check:
    print(f"  {row[0]:12s}: ~{row[1]:.1f}% (n={row[2]:4d})")

print("\n✓ Foundation build complete!")
