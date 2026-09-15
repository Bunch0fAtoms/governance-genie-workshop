# Databricks notebook source
# MAGIC %md
# MAGIC # Pre-built foundation: one worked governance example (arc steps 3 to 5, demonstrated once)
# MAGIC
# MAGIC This runs as part of the GREEN foundation build. It demonstrates the full Attribute-Based
# MAGIC Access Control (ABAC) pattern **once, end to end, on one identifier**, so the room learns by
# MAGIC extending a working example rather than typing boilerplate. The room then repeats the pattern
# MAGIC for the remaining identifiers in `notebooks/todo/phase_2_classification_masks.py`.
# MAGIC
# MAGIC What green pre-builds here:
# MAGIC - **Step 3, comments.** Plain-language comments on `gold_patients` and its governed columns.
# MAGIC - **Step 4, classification.** The governed tag taxonomy (`workshop_pii`, `workshop_access_scope`),
# MAGIC   with one classification applied: `ssn` tagged `workshop_pii=ssn`, `region` tagged
# MAGIC   `workshop_access_scope=region`.
# MAGIC - **Step 5, one ABAC policy of each kind.** One column-mask policy (`mask_ssn_policy`, masking
# MAGIC   `ssn` to the last four digits) and one row-filter policy (`region_row_filter_policy`, scoping
# MAGIC   non-privileged callers to one region).
# MAGIC
# MAGIC **What the room builds next (the second example):** tag and mask `dob` and `patient_name`, and
# MAGIC tokenize the join key `patient_id` across the three gold tables. That extension is the room's
# MAGIC governance step and it sets up the step-10 join.
# MAGIC
# MAGIC **Exempt principals.** ABAC masks everyone in `account users` EXCEPT the privileged principals.
# MAGIC The shipped default is the two account groups from arc step 1 (`workshop_stewards`,
# MAGIC `workshop_admins`). On a workspace where you are not an account admin and cannot create those
# MAGIC groups, pass your own user instead so the policy still creates and masks:
# MAGIC `--var "privileged_principals=you@example.com"`. Unity Catalog resolves only account-level
# MAGIC groups, so a workspace-local group will not work in EXCEPT.

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Primary catalog (required)")
dbutils.widgets.text("sandbox_catalog", "", "Sandbox catalog (defaults to catalog)")
dbutils.widgets.text("schema", "workshop_analytics", "Certified analytics schema")
dbutils.widgets.text("governance_schema", "workshop_governance", "Governance schema")
dbutils.widgets.text("privileged_principals", "workshop_stewards,workshop_admins",
                     "Comma-separated account groups or users exempt from the masks")

catalog = dbutils.widgets.get("catalog")
sandbox_catalog = dbutils.widgets.get("sandbox_catalog") or catalog
schema = dbutils.widgets.get("schema")
governance_schema = dbutils.widgets.get("governance_schema")

if not catalog:
    raise ValueError("catalog is required. Pass --var catalog=<your_catalog> or set the widget.")

analytics = f"{catalog}.{schema}"
gov = f"{catalog}.{governance_schema}"

# Build the EXCEPT clause from the comma-separated principals. Both groups and user emails are
# backtick-quoted. An empty list means no exemption (everyone is masked), which is valid.
_principals = [p.strip() for p in dbutils.widgets.get("privileged_principals").split(",") if p.strip()]
_except = (" EXCEPT " + ", ".join(f"`{p}`" for p in _principals)) if _principals else ""

print(f"Certified analytics : {analytics}")
print(f"Governance          : {gov}")
print(f"Exempt principals   : {_principals or '(none: everyone is masked)'}")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gov}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: recommended comments (the worked example)
# MAGIC
# MAGIC The gold objects are pipeline materialized views, so comments bind with `ALTER MATERIALIZED
# MAGIC VIEW`. If a workspace rejects post-hoc comments on a materialized view, the same comments live
# MAGIC in `pipeline/03_gold.sql`, so the certified surface is documented either way.

# COMMAND ----------

try:
    spark.sql(
        f"ALTER MATERIALIZED VIEW {analytics}.gold_patients "
        f"SET TBLPROPERTIES ('comment' = 'Certified patients: one row per patient, with lifecycle, "
        f"primary channel, referring provider, and 12-month revenue totals. Patient identifiers are "
        f"governed by the ABAC policy set.')"
    )
    print("✓ Table comment: gold_patients")
except Exception as e:
    print(f"⚠ Could not comment gold_patients (documented in the pipeline instead): {str(e)[:100]}")

_column_comments = {
    "ssn": "Social Security Number. Sensitive identifier; masked to the last four digits for non-stewards.",
    "region": "Patient region. Drives the row-level access filter.",
}
for _col, _comment in _column_comments.items():
    try:
        spark.sql(f"ALTER MATERIALIZED VIEW {analytics}.gold_patients ALTER COLUMN {_col} COMMENT '{_comment}'")
        print(f"✓ Column comment: gold_patients.{_col}")
    except Exception as e:
        print(f"⚠ Could not comment gold_patients.{_col}: {str(e)[:100]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: the governed tag taxonomy, and one classification applied
# MAGIC
# MAGIC ABAC matches governed tags only. Governed tags are created once at the account level with
# MAGIC `CREATE GOVERNED TAG` (needs account-level `CREATE`, which account and workspace admins hold).
# MAGIC The tags are namespaced (`workshop_*`) so they never collide with a tag another project already
# MAGIC created. `CREATE GOVERNED TAG` does not accept `IF NOT EXISTS`, so "already exists" is treated
# MAGIC as success.

# COMMAND ----------

_governed_tags = {
    "workshop_pii": ["ssn", "dob", "name", "patient_key"],
    "workshop_access_scope": ["region"],
}
for _tag, _values in _governed_tags.items():
    _values_sql = ", ".join(f"'{v}'" for v in _values)
    try:
        spark.sql(f"CREATE GOVERNED TAG {_tag} VALUES ({_values_sql})")
        print(f"✓ Created governed tag: {_tag} VALUES ({_values_sql})")
    except Exception as e:
        msg = str(e)
        if "ALREADY_EXISTS" in msg or "already exists" in msg.lower():
            print(f"✓ Governed tag {_tag} already exists; reusing it.")
        else:
            print(f"⚠ Could not create governed tag {_tag} (needs account-level CREATE): {msg[:120]}")

# COMMAND ----------

# The green example classifies one PII column (ssn) and the row-filter column (region). The room
# tags dob, patient_name, and the patient_id join key in the participant governance step.
_column_tags = [
    ("gold_patients", "ssn", "workshop_pii", "ssn"),
    ("gold_patients", "region", "workshop_access_scope", "region"),
]
for _tbl, _col, _key, _val in _column_tags:
    try:
        spark.sql(
            f"ALTER MATERIALIZED VIEW {analytics}.{_tbl} "
            f"ALTER COLUMN {_col} SET TAGS ('{_key}' = '{_val}')"
        )
        print(f"✓ Tagged {_tbl}.{_col} with {_key}={_val}")
    except Exception as e:
        print(f"⚠ Could not tag {_tbl}.{_col}: {str(e)[:120]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5a: the two functions for the worked example
# MAGIC
# MAGIC Under ABAC the identity logic lives in the policy, so these are pure transforms. Serverless SQL
# MAGIC user defined functions use `RETURN` (not `AS $$ ... $$`).

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {gov}.mask_last4(value STRING)
RETURNS STRING
DETERMINISTIC
RETURN CONCAT('***-**-', SUBSTR(value, -4))
""")
print("✓ mask_last4")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {gov}.region_row_filter(region STRING)
RETURNS BOOLEAN
DETERMINISTIC
RETURN region = 'US West'
""")
print("✓ region_row_filter")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5b: one column-mask policy and one row-filter policy
# MAGIC
# MAGIC Each policy is schema-scoped and tag-driven, so it fires only on columns carrying the governed
# MAGIC tag. `CREATE OR REPLACE POLICY` is idempotent. Policy creation is best-effort: if the exempt
# MAGIC principal does not exist, the foundation still runs green and reports the gap.

# COMMAND ----------

mask_ddl = f"""
CREATE OR REPLACE POLICY mask_ssn_policy
ON SCHEMA {analytics}
COLUMN MASK {gov}.mask_last4
TO `account users`{_except}
FOR TABLES
MATCH COLUMNS has_tag_value('workshop_pii', 'ssn') AS masked_col
ON COLUMN masked_col
"""
try:
    spark.sql(mask_ddl)
    print("✓ ABAC column-mask policy: mask_ssn_policy (mask_last4 where workshop_pii=ssn)")
except Exception as e:
    print(f"⚠ Could not create mask_ssn_policy: {str(e)[:180]}")

row_ddl = f"""
CREATE OR REPLACE POLICY region_row_filter_policy
ON SCHEMA {analytics}
ROW FILTER {gov}.region_row_filter
TO `account users`{_except}
FOR TABLES
MATCH COLUMNS has_tag_value('workshop_access_scope', 'region') AS region_col
USING COLUMNS (region_col)
"""
try:
    spark.sql(row_ddl)
    print("✓ ABAC row-filter policy: region_row_filter_policy (region scope)")
except Exception as e:
    print(f"⚠ Could not create region_row_filter_policy: {str(e)[:180]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5c: the uncertified sandbox schema (step 10 builds into it)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {sandbox_catalog}.workshop_sandbox")
print(f"✓ Ensured sandbox schema {sandbox_catalog}.workshop_sandbox")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

_total = spark.sql(f"SELECT COUNT(*) AS c FROM {analytics}.gold_patients").collect()[0]["c"]
_west = spark.sql(f"SELECT COUNT(*) AS c FROM {analytics}.gold_patients WHERE region = 'US West'").collect()[0]["c"]
print(f"Total patients               : {_total}")
print(f"Visible to a US West analyst  : {_west}")
print(f"Blocked by the row filter     : {_total - _west}  (total minus your region)")

print(f"\nmask_last4 sample: 521-84-7793 -> {spark.sql(f'''SELECT {gov}.mask_last4('521-84-7793') AS m''').collect()[0]['m']}")
print("\n✓ Green governance example built: comments, one classification, one column mask, one row filter.")
