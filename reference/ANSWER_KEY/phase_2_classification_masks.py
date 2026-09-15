# Databricks notebook source
# MAGIC %md
# MAGIC # Steps 3 to 5: Comments, Data Classification, and one ABAC policy
# MAGIC
# MAGIC This is the certified-data protection layer of the workshop, steps 3 to 5 of the
# MAGIC participant arc (PRD section 5A). It replaces the earlier per-table `ALTER ... SET MASK`
# MAGIC and `ALTER ... SET ROW FILTER` approach with **Attribute-Based Access Control (ABAC)**:
# MAGIC one tag-driven policy that Unity Catalog applies to every column carrying the matching tag.
# MAGIC
# MAGIC **Green vs room split (Option A).** The green foundation now pre-builds one worked example of
# MAGIC this pattern: the tag taxonomy (`workshop_pii`, `workshop_access_scope`), the `ssn` mask
# MAGIC (`mask_last4` + `mask_ssn_policy`), and the region row filter (`region_row_filter` +
# MAGIC `region_row_filter_policy`), in `foundation/06_governance_examples.py`. The participant step
# MAGIC extends that example to `dob`, `patient_name`, and the `patient_id` join key. This answer key
# MAGIC keeps the COMPLETE set (all four column masks plus the row filter) as the SA reference; the
# MAGIC green half is idempotent, so re-running it here is harmless.
# MAGIC
# MAGIC - **Step 3, recommended comments.** Plain-language table and column comments so people and
# MAGIC   Genie read the same definitions. Catalog Explorer can also suggest comments with
# MAGIC   artificial intelligence (AI); this notebook writes them with `ALTER ... COMMENT` so the
# MAGIC   result is deterministic.
# MAGIC - **Step 4, classification.** Data Classification (Generally Available, GA) is the automatic
# MAGIC   mechanism: an AI scan applies `class.*` tags to new tables within about 24 hours. Because a
# MAGIC   24-hour scan cannot gate a live session, we also apply an explicit **governed tag** now, so
# MAGIC   the ABAC policy has something to match immediately.
# MAGIC - **Step 5, one ABAC policy set.** Governed-tag-driven `CREATE POLICY` column masks and one
# MAGIC   row filter, applied to everyone except the steward and admin groups. This is the current
# MAGIC   Databricks-recommended form; the older per-table `SET MASK` / `SET ROW FILTER` stays GA as a
# MAGIC   fallback.
# MAGIC
# MAGIC **The join-key teaching moment (a common question).** One column mask is a **deterministic
# MAGIC token** on the join key `patient_id` (`sha2(patient_id, 256)`). A Unity Catalog column mask is
# MAGIC a query-time function applied over the column as rows are read; it is not a decryptable
# MAGIC ciphertext, so there is no decrypt-per-row step to run a join. Storage encryption, if
# MAGIC configured, is a separate control. Because the mask is deterministic and collision-resistant
# MAGIC (the same identifier always yields the same token, and distinct identifiers do not collide), an
# MAGIC analyst can still join on the protected key and get correct matches, with no decrypt step. A
# MAGIC steward joins on the raw value. The mask function still runs in the query path, so it is a
# MAGIC simple deterministic expression and Tandem should benchmark the join at representative scale.
# MAGIC Step 10 joins the derived payer-segment table back to certified patient data on this protected
# MAGIC key to show the join holds.
# MAGIC
# MAGIC **Verified against live AWS Databricks documentation on 2026-09-14.** ABAC `CREATE POLICY`
# MAGIC (GA) supports tables, materialized views, and streaming tables; `MATCH COLUMNS has_tag_value(...)`
# MAGIC matches governed tags only; governed tags are created with `CREATE GOVERNED TAG` (needs
# MAGIC account-level `CREATE`, which account and workspace admins hold by default and can delegate);
# MAGIC UC column masks are applied after the scan and before the join. The item to confirm on the test
# MAGIC workspace is the pipeline-refresh interaction: the foundation pipeline's run-as identity
# MAGIC must be exempt from these policies so gold refreshes read unmasked source. That, and the step-1
# MAGIC steward/admin groups, are called out at step 5.

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Primary catalog (required)")
dbutils.widgets.text("sandbox_catalog", "", "Sandbox catalog (defaults to catalog)")
dbutils.widgets.text("schema", "workshop_analytics", "Certified analytics schema")
dbutils.widgets.text("governance_schema", "workshop_governance", "Governance schema")
dbutils.widgets.text("sandbox_schema", "workshop_sandbox", "Uncertified sandbox schema")

catalog = dbutils.widgets.get("catalog")
sandbox_catalog = dbutils.widgets.get("sandbox_catalog") or catalog
schema = dbutils.widgets.get("schema")
governance_schema = dbutils.widgets.get("governance_schema")
sandbox_schema = dbutils.widgets.get("sandbox_schema")

if not catalog:
    raise ValueError("catalog is required. Pass --var catalog=<your_catalog> or set the widget.")

analytics = f"{catalog}.{schema}"          # certified gold + metric views
gov = f"{catalog}.{governance_schema}"      # functions, policies, mapping and reference tables
sandbox = f"{sandbox_catalog}.{sandbox_schema}"  # uncertified experiment (step 10 derived product)

# The two privileged groups from step 1 (federated in the account console). Everyone else is a
# non-privileged analyst and sees masked, row-filtered data. The policies below exempt these groups.
# The foundation pipeline's run-as identity must belong to one of them, so that when the pipeline
# refreshes the gold materialized views it reads unmasked source data (see step 5b).
PRIVILEGED_GROUPS = ["workshop_stewards", "workshop_admins"]

print(f"Certified analytics : {analytics}")
print(f"Governance          : {gov}")
print(f"Sandbox             : {sandbox}")
print(f"Privileged groups   : {', '.join(PRIVILEGED_GROUPS)}")

# COMMAND ----------

# Ensure the governance schema exists (functions, policies, and reference tables live here).
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gov}")
print(f"✓ Ensured governance schema {gov}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: recommended comments on the certified tables and columns
# MAGIC
# MAGIC The gold objects are pipeline materialized views, so comments bind with
# MAGIC `ALTER MATERIALIZED VIEW`. If a workspace does not accept post-hoc comments on a materialized
# MAGIC view, the same comments live in the pipeline definition (`pipeline/03_gold.sql`), so the
# MAGIC certified surface is documented either way. Catalog Explorer's AI-recommended comments are the
# MAGIC point-and-click equivalent a steward can use in the user interface.

# COMMAND ----------

# Table-level comments (idempotent). gold_patients / gold_claims / gold_revenue_summary / gold_hcp
# are materialized views; ALTER MATERIALIZED VIEW is the correct verb.
_table_comments = {
    "gold_patients": "Certified patients: one row per patient, with lifecycle, primary channel, referring provider, and 12-month revenue totals. Patient identifiers are governed (see the ABAC policy).",
    "gold_claims": "Certified claims: pump-on-body and consumable-shipment events, with cycle time and month offset from placement.",
    "gold_revenue_summary": "Certified revenue: per-patient, per-month recognized revenue, margin, and cumulative growth.",
    "gold_hcp": "Certified healthcare providers: commercial profile with referral and conversion metrics.",
}
for _obj, _comment in _table_comments.items():
    try:
        spark.sql(f"ALTER MATERIALIZED VIEW {analytics}.{_obj} SET TBLPROPERTIES ('comment' = '{_comment}')")
        print(f"✓ Comment set (materialized view): {_obj}")
    except Exception as e:
        print(f"⚠ Could not comment {_obj} (documented in the pipeline instead): {str(e)[:100]}")

# COMMAND ----------

# Column-level comments on the governed identifier columns of gold_patients, so the definition and
# the sensitivity travel together.
_column_comments = {
    "patient_id": "Pseudonymous patient key. Governed join key: protected by a deterministic-token column mask so it stays joinable without decryption.",
    "ssn": "Social Security Number. Sensitive identifier; masked to the last four digits for non-stewards.",
    "dob": "Date of birth. Sensitive; generalized to January 1 of the birth year for non-stewards.",
    "patient_name": "Patient full name. Sensitive; redacted for non-stewards.",
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
# MAGIC ## Step 4a: Data Classification, the automatic mechanism
# MAGIC
# MAGIC Data Classification (GA) is how tagging happens without anyone hand-labelling columns. An AI
# MAGIC scan reads new tables and applies `class.*` tags (for example `class.ssn`, `class.name`).
# MAGIC Enable it per catalog or schema in Catalog Explorer, with the `databricks data-classification`
# MAGIC command line, or through the Application Programming Interface (API). The scan runs within
# MAGIC about 24 hours, so it documents and demonstrates the mechanism; it does not fire inside a live
# MAGIC session. The next cell applies an explicit governed tag so the ABAC policy has a match now.

# COMMAND ----------

print(
    "Data Classification is enabled at the catalog or schema level (Catalog Explorer, the\n"
    "`databricks data-classification` CLI, or the API). It applies class.* tags on a ~24h scan.\n"
    "For a deterministic live gate, the workshop also applies an explicit governed tag below."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4b: the governed tag taxonomy
# MAGIC
# MAGIC ABAC matches **governed tags** only, not free-text tags. Governed tags are created once at the
# MAGIC account level with `CREATE GOVERNED TAG` (needs account-level `CREATE`). This pairs naturally
# MAGIC with step 1, where an account admin sets up the workshop in the account console. If the
# MAGIC running principal lacks account-level `CREATE`, this cell reports it so an admin can create the
# MAGIC tags, then the notebook can be re-run from here.
# MAGIC
# MAGIC - `pii` classifies the sensitive identifier columns. The value names the handling: `ssn`,
# MAGIC   `dob`, `name`, and `patient_key` (the deterministic-token join key).
# MAGIC - `access_scope` marks the column that drives row-level access (`region`).

# COMMAND ----------

# The governed tags are namespaced (`workshop_*`) so they never collide with a tag another
# project already created at the account level. On the shared a serverless workspace test account, generic
# `pii` and `PII` governed tags already existed with different values, and mutating another
# team's account-level tag is not acceptable, so the workshop owns its own namespaced tags
# (verified).
_governed_tags = {
    "workshop_pii": ["ssn", "dob", "name", "patient_key"],
    "workshop_access_scope": ["region"],
}
for _tag, _values in _governed_tags.items():
    _values_sql = ", ".join(f"'{v}'" for v in _values)
    # Note: CREATE GOVERNED TAG does NOT accept IF NOT EXISTS (verified).
    # Create it, and treat "already exists" as success so the notebook is idempotent.
    try:
        spark.sql(f"CREATE GOVERNED TAG {_tag} VALUES ({_values_sql})")
        print(f"✓ Created governed tag: {_tag} VALUES ({_values_sql})")
    except Exception as e:
        msg = str(e)
        if "ALREADY_EXISTS" in msg or "already exists" in msg.lower():
            print(f"✓ Governed tag {_tag} already exists; reusing it.")
        else:
            print(
                f"⚠ Could not create governed tag {_tag}: {msg[:140]}\n"
                f"  This needs account-level CREATE (account and workspace admins hold it by default;\n"
                f"  it can be delegated). A principal with that privilege runs:\n"
                f"    CREATE GOVERNED TAG {_tag} VALUES ({_values_sql});\n"
                f"  then re-run this notebook from Step 4b."
            )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4c: assign the governed tags to columns
# MAGIC
# MAGIC Column tags do not inherit from the table, so each column is tagged directly. The gold objects
# MAGIC are materialized views, so `ALTER MATERIALIZED VIEW ... ALTER COLUMN ... SET TAGS` binds them.

# COMMAND ----------

# (table, column, governed_tag_key, governed_tag_value)
# The deterministic-token join key must be tagged on EVERY certified table that exposes
# patient_id, or a non-privileged analyst's cross-table joins compare a token to a raw value
# and silently drop every row (verified). So patient_id carries the
# patient_key tag on gold_patients, gold_revenue_summary, and gold_claims. The other PII
# columns and the row-filter column live only on gold_patients.
_column_tags = [
    ("gold_patients", "ssn", "workshop_pii", "ssn"),
    ("gold_patients", "dob", "workshop_pii", "dob"),
    ("gold_patients", "patient_name", "workshop_pii", "name"),
    ("gold_patients", "patient_id", "workshop_pii", "patient_key"),
    ("gold_patients", "region", "workshop_access_scope", "region"),
    ("gold_revenue_summary", "patient_id", "workshop_pii", "patient_key"),
    ("gold_claims", "patient_id", "workshop_pii", "patient_key"),
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
# MAGIC ## Solutions Architect notes: confirm before finalizing step 5 on real data
# MAGIC
# MAGIC The deterministic-token join key works cleanly on the synthetic foundation. Before applying
# MAGIC the same pattern to Tandem's real data, the Solutions Architect should confirm:
# MAGIC
# MAGIC 1. **One canonical format for the join key.** `patient_id` must have the same format on both
# MAGIC    sides of every join, across the certified and derived tables. The token only matches when
# MAGIC    the inputs are identical, so a stray prefix, casing, or padding difference silently drops
# MAGIC    matches. Normalize the key before tokenizing if the formats differ.
# MAGIC 2. **Null handling.** Confirm whether the join key can be null and how nulls should behave.
# MAGIC    `sha2(NULL, 256)` is null, and null never equals null in a join, so null keys drop out.
# MAGIC    Decide whether that is correct or whether nulls need a sentinel.
# MAGIC 3. **Display versus match.** Decide whether a masked analyst needs to select and display the
# MAGIC    pseudonym, or only use it to match rows. If display is not required, the token can be
# MAGIC    join-only, which is a stronger privacy posture.
# MAGIC 4. **Guessing-attack resistance.** The workshop uses a bare `sha2` for teaching clarity. A
# MAGIC    bare public hash of a predictable identifier (for example a sequential id) is guessable by
# MAGIC    hashing candidate values. If Tandem requires protection against that, do not use a bare
# MAGIC    hash: use a keyed hash (a keyed-hash message authentication code, HMAC, with a secret held
# MAGIC    in a secret scope) or a tokenization service, so the token cannot be reproduced without the
# MAGIC    key. Databricks documents consistent hashing / deterministic pseudonymization as the
# MAGIC    cross-table pattern this builds on.
# MAGIC 5. **Pipeline refresh identity (the load-bearing one for a gold materialized view).** When the
# MAGIC    foundation pipeline refreshes a gold materialized view, the refresh runs as the pipeline's
# MAGIC    run-as identity. If that identity is subject to these masks or filters on the source it
# MAGIC    reads, the refresh can fail or materialize protected values into gold. So exempt the pipeline
# MAGIC    run-as identity from any policy covering its source (here, by keeping it in an exempt group),
# MAGIC    let the refresh build gold from unmasked source, and apply the consumer-facing policy to the
# MAGIC    gold result. Test with both the pipeline identity and a restricted analyst identity.
# MAGIC 6. **Return type and multi-type columns.** A column mask should return the column's own type
# MAGIC    (for a struct, the same struct type, or operations like `MERGE` break). A string token fits a
# MAGIC    string key; a numeric key needs a type-compatible token or a separate persisted token column.
# MAGIC    To mask many columns of different types with one rule, write a single user defined function
# MAGIC    that accepts and returns `VARIANT`; Databricks casts its output to each target column's type.
# MAGIC 7. **Pipeline-managed masks use `CREATE`, not `ALTER`.** These policies are schema-scoped ABAC,
# MAGIC    which needs no `ALTER` on the materialized view. If a workspace instead needs object-level
# MAGIC    masking on a pipeline-managed materialized view, define the `MASK` / `ROW FILTER` in the
# MAGIC    `CREATE MATERIALIZED VIEW` (in `pipeline/03_gold.sql`), because post-creation `ALTER` masking
# MAGIC    is unreliable for pipeline-managed views.
# MAGIC
# MAGIC These are the "use your own data" checks; on the synthetic kit the bare token is intentional.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5a: the mask and filter functions (pure transforms)
# MAGIC
# MAGIC Under ABAC the identity logic lives in the policy (`TO ... EXCEPT`), so these functions are
# MAGIC pure transformations. Each mask returns the column's own type. The join-key mask is a
# MAGIC deterministic token: `sha2` returns the same 64-character value for the same input every time,
# MAGIC so joins on the token still match, and it is collision-resistant, so `COUNT(DISTINCT ...)`
# MAGIC over the token equals the count over the raw key. Functions use `RETURN` (serverless SQL user
# MAGIC defined functions do not accept `AS $$ ... $$`).

# COMMAND ----------

# Show the last four characters of a string identifier (ssn).
spark.sql(f"""
CREATE OR REPLACE FUNCTION {gov}.mask_last4(value STRING)
RETURNS STRING
DETERMINISTIC
RETURN CONCAT('***-**-', SUBSTR(value, -4))
""")
print("✓ mask_last4")

# Generalize a date of birth to January 1 of the birth year (returns DATE to match the column).
spark.sql(f"""
CREATE OR REPLACE FUNCTION {gov}.mask_dob_year(value DATE)
RETURNS DATE
DETERMINISTIC
RETURN make_date(YEAR(value), 1, 1)
""")
print("✓ mask_dob_year")

# Redact a full name.
spark.sql(f"""
CREATE OR REPLACE FUNCTION {gov}.mask_redact(value STRING)
RETURNS STRING
DETERMINISTIC
RETURN '[Redacted]'
""")
print("✓ mask_redact")

# Deterministic token for the join key: same input -> same token, no decryption to join.
# Bare sha2 is a teaching simplification; for a guessable real identifier use a keyed HMAC or a
# tokenization service instead (see Solutions Architect note 4 above).
spark.sql(f"""
CREATE OR REPLACE FUNCTION {gov}.tokenize_key(value STRING)
RETURNS STRING
DETERMINISTIC
RETURN sha2(value, 256)
""")
print("✓ tokenize_key")

# Row filter: non-privileged callers are scoped to one region. Pure predicate; the policy applies
# it only to non-stewards. The production form reads a principal-to-region mapping table (below);
# the workshop uses a fixed region so the blocked-row count is a deterministic unlock value.
spark.sql(f"""
CREATE OR REPLACE FUNCTION {gov}.region_row_filter(region STRING)
RETURNS BOOLEAN
DETERMINISTIC
RETURN region = 'US West'
""")
print("✓ region_row_filter")

# COMMAND ----------

# Production reference: a principal-to-region mapping the row filter can read instead of a fixed
# region. Kept as the generalization pattern; the workshop filter above uses the fixed region for a
# deterministic gate. Emails are example.com (no real principals in the kit).
_region_map = [
    ("analyst@example.com", "US West"),
    ("steward@example.com", "ALL"),
]
spark.createDataFrame(_region_map, schema=["principal_id", "assigned_region"]) \
    .write.mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{gov}.user_region_assignment")
print(f"✓ Reference mapping table {gov}.user_region_assignment")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5b: one ABAC policy set, driven by the governed tags
# MAGIC
# MAGIC Each policy applies a function to every column carrying the matching governed tag, for everyone
# MAGIC except the steward and admin groups. `CREATE OR REPLACE POLICY` makes this idempotent.
# MAGIC Policies are created at schema scope `FOR TABLES`.
# MAGIC
# MAGIC ABAC policies support tables, materialized views, and streaming tables, so a schema-scoped
# MAGIC policy covers the gold materialized views. `MATCH COLUMNS` fires only on columns that carry the
# MAGIC governed tag, so only the tagged gold columns are affected; the untagged silver source stays
# MAGIC untouched.
# MAGIC
# MAGIC **Two prerequisites, confirmed on the test workspace, not assumed:**
# MAGIC 1. The groups `workshop_stewards` and `workshop_admins` exist (step 1). A policy that names a
# MAGIC    missing group in `EXCEPT` fails, so this cell reports that clearly.
# MAGIC 2. **Pipeline-refresh identity.** The foundation pipeline's run-as identity must be exempt from
# MAGIC    any policy covering the source it reads, so a gold refresh builds from unmasked data rather
# MAGIC    than failing or baking protected values into gold. Here that identity is kept in an exempt
# MAGIC    group. Confirm on a serverless workspace that a refresh after the policy is applied still produces correct
# MAGIC    gold, and that a restricted analyst sees the mask. If a workspace ever needs object-level
# MAGIC    masking on a pipeline-managed materialized view instead, define the mask in the
# MAGIC    `CREATE MATERIALIZED VIEW` (`pipeline/03_gold.sql`), since post-creation `ALTER` masking is
# MAGIC    unreliable for pipeline-managed views.

# COMMAND ----------

_except = ", ".join(f"`{g}`" for g in PRIVILEGED_GROUPS)

# Column-mask policies: one per governed-tag value, each mapping to the right function.
_mask_policies = [
    ("mask_ssn_policy", "mask_last4", "workshop_pii", "ssn"),
    ("mask_dob_policy", "mask_dob_year", "workshop_pii", "dob"),
    ("mask_name_policy", "mask_redact", "workshop_pii", "name"),
    ("mask_patient_key_policy", "tokenize_key", "workshop_pii", "patient_key"),
]
for _policy, _fn, _key, _val in _mask_policies:
    ddl = f"""
CREATE OR REPLACE POLICY {_policy}
ON SCHEMA {analytics}
COLUMN MASK {gov}.{_fn}
TO `account users` EXCEPT {_except}
FOR TABLES
MATCH COLUMNS has_tag_value('{_key}', '{_val}') AS masked_col
ON COLUMN masked_col
"""
    try:
        spark.sql(ddl)
        print(f"✓ ABAC column-mask policy: {_policy} ({_fn} where {_key}={_val})")
    except Exception as e:
        print(f"⚠ Could not create {_policy}: {str(e)[:160]}")

# COMMAND ----------

# Row-filter policy: scope non-stewards to their region.
row_ddl = f"""
CREATE OR REPLACE POLICY region_row_filter_policy
ON SCHEMA {analytics}
ROW FILTER {gov}.region_row_filter
TO `account users` EXCEPT {_except}
FOR TABLES
MATCH COLUMNS has_tag_value('workshop_access_scope', 'region') AS region_col
USING COLUMNS (region_col)
"""
try:
    spark.sql(row_ddl)
    print("✓ ABAC row-filter policy: region_row_filter_policy (region scope)")
except Exception as e:
    print(f"⚠ Could not create region_row_filter_policy: {str(e)[:160]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5c: the uncertified sandbox schema for step 10
# MAGIC
# MAGIC Read-in-place model: the foundation makes no physical copies of the certified tables. The one
# MAGIC uncertified derived product, `workshop_sandbox.patient_payer_segment`, is built live by the
# MAGIC room in step 10, reading certified `gold_patients` in place so Unity Catalog keeps a one-hop
# MAGIC lineage back to a certified source. Here we only ensure the schema exists.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {sandbox}")
print(f"✓ Ensured sandbox schema {sandbox} (derived payer-segment product is built live in step 10)")

# COMMAND ----------

# The step-10 derived product carries the join key patient_id too, so the same deterministic
# token must protect it, or an analyst joining the derived table to certified data would compare
# a token (certified side) to a raw value (sandbox side) and drop every row. Replicate the
# patient_key tokenize policy on the sandbox schema so the derived join key tokenizes for the same
# non-privileged principals. It fires only on a column carrying the patient_key tag, which step 10
# sets on patient_payer_segment.patient_id.
try:
    spark.sql(f"""
CREATE OR REPLACE POLICY mask_patient_key_policy
ON SCHEMA {sandbox}
COLUMN MASK {gov}.tokenize_key
TO `account users` EXCEPT {_except}
FOR TABLES
MATCH COLUMNS has_tag_value('workshop_pii', 'patient_key') AS c
ON COLUMN c
""")
    print(f"✓ ABAC tokenize policy replicated on the sandbox schema {sandbox}")
except Exception as e:
    print(f"⚠ Could not create sandbox mask_patient_key_policy: {str(e)[:160]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification and the join-key answer
# MAGIC
# MAGIC The counts below are the Level 2 unlock values. Run them as a privileged user so they report
# MAGIC the true totals, not a filtered view.
# MAGIC
# MAGIC **How to see the masks:** query `gold_patients` as an analyst and as a steward. The analyst
# MAGIC sees `ssn` as `***-**-1234`, `dob` as the January-1 year, `patient_name` as `[Redacted]`,
# MAGIC and `patient_id` as the 64-character token. The steward sees the raw values.
# MAGIC
# MAGIC **The join answer:** an analyst joins the step-10 derived table to `gold_patients`
# MAGIC on `patient_id`. Both sides carry the same deterministic token, so the join matches correctly.
# MAGIC There is no decryption, because UC masking is a read-time transformation, not encryption at
# MAGIC rest. A non-deterministic mask (redaction, last-4) would collapse distinct keys and break the
# MAGIC join, creating false matches. That is exactly why the join key uses a deterministic token.

# COMMAND ----------

_total = spark.sql(f"SELECT COUNT(*) AS c FROM {analytics}.gold_patients").collect()[0]["c"]
_west = spark.sql(
    f"SELECT COUNT(*) AS c FROM {analytics}.gold_patients WHERE region = 'US West'"
).collect()[0]["c"]
print(f"Total patients                  : {_total}")
print(f"Visible to a US West analyst     : {_west}")
print(f"Blocked by the row filter        : {_total - _west}  <- Level 2 unlock value")

# Show the deterministic token is stable and collision-free over the key (distinct counts match).
_distinct = spark.sql(f"""
SELECT COUNT(DISTINCT patient_id) AS raw_distinct,
       COUNT(DISTINCT sha2(patient_id, 256)) AS token_distinct
FROM {analytics}.gold_patients
""").collect()[0]
print(f"Distinct patient_id (raw)        : {_distinct['raw_distinct']}")
print(f"Distinct token (sha2)            : {_distinct['token_distinct']}  <- equal, so the token is a safe join key")

print("\n✓ Steps 3 to 5 complete: comments, governed classification, and one ABAC policy set.")
