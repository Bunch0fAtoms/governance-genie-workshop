# Databricks notebook source
# MAGIC %md
# MAGIC # Steps 3 to 5: extend the governance pattern (the second example)
# MAGIC
# MAGIC The green foundation already demonstrated the full Attribute-Based Access Control (ABAC) pattern
# MAGIC once, end to end, on one identifier. It pre-built:
# MAGIC - comments on `gold_patients` (the `ssn` and `region` columns),
# MAGIC - the governed tag taxonomy `workshop_pii` (`ssn`, `dob`, `name`, `patient_key`) and
# MAGIC   `workshop_access_scope` (`region`),
# MAGIC - one classification applied (`ssn` tagged `workshop_pii=ssn`, `region` tagged
# MAGIC   `workshop_access_scope=region`),
# MAGIC - the `mask_last4` function and `region_row_filter` function,
# MAGIC - one column-mask policy (`mask_ssn_policy`) and one row-filter policy (`region_row_filter_policy`).
# MAGIC
# MAGIC **Your job is to extend that working example to the remaining identifiers.** Build it with Genie
# MAGIC Code or by hand. The exact solution is in `reference/ANSWER_KEY/phase_2_classification_masks.py`;
# MAGIC this notebook is the facts-only specification. All objects are the pre-built gold materialized
# MAGIC views in `workshop_analytics`, so tags and comments bind with `ALTER MATERIALIZED VIEW`.

# COMMAND ----------

dbutils.widgets.text("catalog", "workshop_certified", "Primary catalog")
dbutils.widgets.text("sandbox_catalog", "workshop_sandbox", "Sandbox catalog (defaults to catalog)")
dbutils.widgets.text("schema", "workshop_analytics", "Certified analytics schema")
dbutils.widgets.text("governance_schema", "workshop_governance", "Governance schema")
dbutils.widgets.text("sandbox_schema", "workshop_sandbox", "Uncertified sandbox schema")

catalog = dbutils.widgets.get("catalog")
sandbox_catalog = dbutils.widgets.get("sandbox_catalog") or catalog
schema = dbutils.widgets.get("schema")
governance_schema = dbutils.widgets.get("governance_schema")
sandbox_schema = dbutils.widgets.get("sandbox_schema")

analytics = f"{catalog}.{schema}"
gov = f"{catalog}.{governance_schema}"
sandbox = f"{sandbox_catalog}.{sandbox_schema}"
print(f"Certified analytics : {analytics}\nGovernance          : {gov}\nSandbox             : {sandbox}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 TODO: comments on the remaining governed columns
# MAGIC
# MAGIC Green commented `ssn` and `region`. Add plain-language comments for the identifiers you are
# MAGIC about to protect, so the definition and the sensitivity travel together.
# MAGIC - Column comments on `gold_patients` for `patient_id` (the pseudonymous join key), `dob`, and
# MAGIC   `patient_name`.
# MAGIC - Table comments on `gold_claims`, `gold_revenue_summary`, and `gold_hcp`.
# MAGIC - Verb: `ALTER MATERIALIZED VIEW ... SET TBLPROPERTIES ('comment' = ...)` for the table, and
# MAGIC   `ALTER MATERIALIZED VIEW ... ALTER COLUMN ... COMMENT ...` for a column.

# COMMAND ----------

# TODO: step 3 comments for patient_id, dob, patient_name, and the other gold tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 TODO: classify the remaining columns
# MAGIC
# MAGIC The governed tags already exist (green created the taxonomy). Tag the columns green left for you,
# MAGIC reusing the same `workshop_pii` values:
# MAGIC - `gold_patients.dob` -> `workshop_pii=dob`
# MAGIC - `gold_patients.patient_name` -> `workshop_pii=name`
# MAGIC - The deterministic-token join key `patient_id` -> `workshop_pii=patient_key` on EVERY table that
# MAGIC   exposes it (`gold_patients`, `gold_revenue_summary`, `gold_claims`), or a masked analyst's
# MAGIC   cross-table joins compare a token to a raw value and drop every row.

# COMMAND ----------

# TODO: step 4 column tags for dob, patient_name, and patient_id (three tables).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 TODO: the remaining ABAC policies, plus the sandbox join key
# MAGIC
# MAGIC Green built `mask_last4` + `mask_ssn_policy` and the `region_row_filter` + `region_row_filter_policy`.
# MAGIC Add the rest, following the same shape. Serverless SQL functions use `RETURN`, not `AS $$ ... $$`,
# MAGIC and each mask returns the column's own type.
# MAGIC - Functions in `workshop_governance`: `mask_dob_year` (dob -> January 1 of the birth year,
# MAGIC   returns DATE), `mask_redact` (name -> `[Redacted]`), `tokenize_key` (`sha2(value, 256)`, the
# MAGIC   deterministic join-key token).
# MAGIC - Three more column-mask policies (`mask_dob_policy`, `mask_name_policy`, `mask_patient_key_policy`),
# MAGIC   created `ON SCHEMA workshop_analytics`, `TO account users EXCEPT workshop_stewards, workshop_admins`,
# MAGIC   `FOR TABLES`, matched by `has_tag_value('workshop_pii', ...)`. The two groups come from step 1 and
# MAGIC   must be account-level groups, or Unity Catalog cannot resolve them.
# MAGIC - Replicate the `tokenize_key` policy `ON SCHEMA workshop_sandbox`, so the step-10 derived
# MAGIC   product's join key tokenizes consistently with the certified side.
# MAGIC - Constraint: keep the pipeline run-as identity exempt (in a steward or admin group) so a gold
# MAGIC   refresh reads unmasked source.

# COMMAND ----------

# TODO: step 5 functions (mask_dob_year, mask_redact, tokenize_key), their ABAC policies, and the
# sandbox tokenize policy.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Level 2 gate: the unlock values
# MAGIC
# MAGIC Run as a non-privileged analyst and as a steward, then report:
# MAGIC 1. The masked value a non-privileged analyst sees for `patient_name` (expect `[Redacted]`) and
# MAGIC    the tokenized `patient_id`.
# MAGIC 2. The blocked-row count the region filter enforces on `gold_patients` (total minus the rows in
# MAGIC    the analyst's region).

# COMMAND ----------

# TODO: run the two gate checks and enter the unlock values.
