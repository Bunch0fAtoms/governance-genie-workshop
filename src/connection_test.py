# Databricks notebook source
# MAGIC %md
# MAGIC # Connection and capability probe
# MAGIC
# MAGIC Confirms you can run the workshop build, not just connect. It runs each
# MAGIC governance primitive the workshop uses against a temporary schema, records a
# MAGIC pass or fail per capability, then drops the schema so nothing is left behind.
# MAGIC
# MAGIC What it checks: create schema, create table and write, column mask, row
# MAGIC filter, tags, and grant/revoke. It prints a report at the end and exits with
# MAGIC that report. If any check fails, the job ends FAILED and names what failed.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "A catalog you can create a schema in")
dbutils.widgets.text("schema", "connection_test", "Temporary schema name")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
fq = f"{catalog}.{schema}"
print(f"Probing against {fq}\n")

# COMMAND ----------

results = []  # (capability, "PASS"/"FAIL", detail)


def probe(name, fn):
    try:
        fn()
        results.append((name, "PASS", ""))
        print(f"  [PASS] {name}")
    except Exception as e:
        detail = str(e).splitlines()[0][:180]
        results.append((name, "FAIL", detail))
        print(f"  [FAIL] {name}: {detail}")


def do_schema():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fq} COMMENT 'connection test, safe to drop'")


def do_table():
    spark.sql(f"CREATE TABLE IF NOT EXISTS {fq}.ping (id INT, note STRING)")
    spark.sql(f"INSERT INTO {fq}.ping VALUES (1, 'connection test')")
    assert spark.sql(f"SELECT COUNT(*) c FROM {fq}.ping").collect()[0]["c"] >= 1


def do_column_mask():
    # SQL UDF must use RETURN (not AS $$..$$) and return the column's type.
    spark.sql(f"CREATE OR REPLACE FUNCTION {fq}.mask_note(v STRING) RETURN '***'")
    spark.sql(f"ALTER TABLE {fq}.ping ALTER COLUMN note SET MASK {fq}.mask_note")
    got = spark.sql(f"SELECT note FROM {fq}.ping LIMIT 1").collect()[0]["note"]
    assert got == "***", f"mask did not apply, read {got!r}"


def do_row_filter():
    spark.sql(f"CREATE OR REPLACE FUNCTION {fq}.rf(id INT) RETURN id >= 0")
    spark.sql(f"ALTER TABLE {fq}.ping SET ROW FILTER {fq}.rf ON (id)")
    spark.sql(f"SELECT * FROM {fq}.ping").collect()  # still readable with the filter on


def do_tags():
    # Use a test-specific tag key. A common key like "classification" may carry a
    # UC tag policy that restricts its values, which would fail for reasons
    # unrelated to whether you can set tags at all.
    spark.sql(f"ALTER TABLE {fq}.ping SET TAGS ('connection_test' = 'true')")
    spark.sql(f"ALTER TABLE {fq}.ping ALTER COLUMN note SET TAGS ('connection_test' = 'true')")


def do_grant():
    spark.sql(f"GRANT USE SCHEMA ON SCHEMA {fq} TO `account users`")
    spark.sql(f"REVOKE USE SCHEMA ON SCHEMA {fq} FROM `account users`")


# COMMAND ----------

print("Running probes:")
probe("create schema", do_schema)
probe("create table and write", do_table)
probe("column mask (SET MASK)", do_column_mask)
probe("row filter (SET ROW FILTER)", do_row_filter)
probe("tags (SET TAGS)", do_tags)
probe("grant / revoke", do_grant)

# COMMAND ----------

# Clean up: dropping the schema removes the table and the functions with it.
try:
    spark.sql(f"DROP SCHEMA IF EXISTS {fq} CASCADE")
    print(f"\nCleaned up {fq}. Nothing left behind.")
except Exception as e:
    print(f"\nCleanup warning (you may need to drop {fq} by hand): {str(e).splitlines()[0]}")

# COMMAND ----------

passed = sum(1 for _, s, _ in results if s == "PASS")
fails = [(n, d) for n, s, d in results if s == "FAIL"]
overall = "PASSED" if not fails else "FAILED"

lines = [f"  {s:4}  {n}" + (f"  ({d})" if d else "") for n, s, d in results]
report = "\n".join(
    [
        f"RESULT: {overall}  ({passed}/{len(results)} checks passed)",
        f"Target: {fq}",
        "-" * 60,
        *lines,
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Result
# MAGIC The table and text below are the report. If you ran this with **Run all** in
# MAGIC the notebook, it shows right here. If you ran it as a job, it shows on the
# MAGIC run page. Reply to us with this result.

# COMMAND ----------

# A rendered table, visible in the notebook UI and on the run page.
display(spark.createDataFrame(results, ["capability", "status", "detail"]))
print("\n" + report + "\n")

# COMMAND ----------

# Also return the report as the notebook result, so it shows at the end of the CLI
# `bundle run` and in the run's Output. On failure, raise so the job state is FAILED.
if fails:
    raise Exception("Capability probe FAILED.\n" + report)
dbutils.notebook.exit(report)
