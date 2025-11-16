# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b9bfdb39-54b5-4f3b-8233-5ce45416fc7d",
# META       "default_lakehouse_name": "LH_DE_Bronze",
# META       "default_lakehouse_workspace_id": "9863d5a3-3b74-428f-ae1d-9ca030333569",
# META       "known_lakehouses": [
# META         {
# META           "id": "b9bfdb39-54b5-4f3b-8233-5ce45416fc7d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# -------------------------------------------------------------
# 2️. AUDIT_BRONZE_LOADS  (Audit of every ingestion run)
# -------------------------------------------------------------

spark.sql("""
CREATE TABLE IF NOT EXISTS AUDIT_BRONZE_LOADS (
  audit_id            STRING,
  domain              STRING,
  table_name          STRING,
  target_table        STRING,
  source_path         STRING,
  rows_ingested       BIGINT,
  rows_quarantined    BIGINT,
  load_start_ts_utc   TIMESTAMP,
  load_end_ts_utc     TIMESTAMP,
  load_status         STRING,
  error_message       STRING,
  run_id              STRING,
  created_by          STRING,
  created_ts_utc      TIMESTAMP
) USING DELTA;
""")

print("✅ Created AUDIT_BRONZE_LOADS table")




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
