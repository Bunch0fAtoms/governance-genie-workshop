# Databricks notebook source
# MAGIC %md
# MAGIC # Connection Test
# MAGIC
# MAGIC If this notebook finishes, four things work:
# MAGIC 1. You pulled the repo.
# MAGIC 2. Your Databricks command-line interface (CLI) is authenticated to the workspace.
# MAGIC 3. The bundle deployed.
# MAGIC 4. A job ran on compute.
# MAGIC
# MAGIC It is read-only. It creates and changes nothing in your data.

# COMMAND ----------

from datetime import datetime

print("Bundle deployed and the job is running.")
print(f"Workspace clock: {datetime.now().isoformat()}")

# COMMAND ----------

# A small read-only query confirms Spark and Unity Catalog are reachable.
df = spark.sql(
    "SELECT current_user() AS user, current_catalog() AS catalog, current_timestamp() AS run_time"
)
df.show(truncate=False)

# COMMAND ----------

print("Connection test passed. You are ready for the workshop build.")
