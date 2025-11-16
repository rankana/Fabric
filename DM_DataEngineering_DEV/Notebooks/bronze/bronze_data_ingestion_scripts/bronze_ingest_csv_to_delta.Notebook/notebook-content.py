# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# %run /_bronze_utils
# -------------------------------------------------------------
# Bronze Ingestion Notebook (Generic CSV → Delta)
# -------------------------------------------------------------
# Parameters passed by pipeline:
#   domain, table_name, target_table, schema_path, input_glob, delimiter
# -------------------------------------------------------------

from pyspark.sql import functions as F
import json, datetime, uuid

# --- Receive pipeline parameters ---
dbutils.widgets.text("domain", "")
dbutils.widgets.text("table_name", "")
dbutils.widgets.text("target_table", "")
dbutils.widgets.text("schema_path", "")
dbutils.widgets.text("input_glob", "")
dbutils.widgets.text("delimiter", "|")

domain        = dbutils.widgets.get("domain")
table_name    = dbutils.widgets.get("table_name")
target_table  = dbutils.widgets.get("target_table")
schema_path   = dbutils.widgets.get("schema_path")
input_glob    = dbutils.widgets.get("input_glob")
delimiter     = dbutils.widgets.get("delimiter")

run_id = str(uuid.uuid4())
load_start = datetime.datetime.utcnow()

print(f"🚀 Starting Bronze ingestion for {table_name} ({domain})")

# --- Load expected schema ---
schema_json = spark.read.text(schema_path).collect()[0][0]
schema_def = json.loads(schema_json)
expected_cols = schema_def.get("expected_columns", [])

# --- Read incoming files ---
df_raw = (
    spark.read
    .option("header", True)
    .option("delimiter", delimiter)
    .csv(input_glob)
)

# --- Schema alignment ---
missing_cols = [c for c in expected_cols if c not in df_raw.columns]
for c in missing_cols:
    df_raw = df_raw.withColumn(c, F.lit(None))

df_raw = df_raw.select(expected_cols)

# --- Add metadata columns ---
df_final = (df_raw
    .withColumn("SOURCE_SYSTEM", F.lit("OracleHealth"))
    .withColumn("LOAD_RUN_ID", F.lit(run_id))
    .withColumn("LOAD_TS_UTC", F.current_timestamp())
    .withColumn("FILE_NAME", F.input_file_name())
    .withColumn("RECORD_HASH", F.sha2(F.concat_ws("|", *expected_cols), 256))
)

# --- Write to Delta table ---
try:
    df_final.write.format("delta").mode("append").saveAsTable(target_table)
    status = "SUCCESS"
    error_msg = None
    quarantined = 0
except Exception as e:
    status = "FAILED"
    error_msg = str(e)
    quarantined = df_final.count()

# --- Audit logging ---
rows_written = df_final.count()
load_end = datetime.datetime.utcnow()

spark.sql(f"""
INSERT INTO AUDIT_BRONZE_LOADS VALUES (
  '{uuid.uuid4()}',
  '{domain}', '{table_name}', '{target_table}',
  '{input_glob}', {rows_written}, {quarantined},
  timestamp('{load_start}'), timestamp('{load_end}'),
  '{status}', '{error_msg}', '{run_id}', 'FabricPipeline', current_timestamp()
)
""")

print(f"✅ Bronze ingestion complete for {table_name}: {status}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
