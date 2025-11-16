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
# 3️. DQ_QUARANTINE_LOGS  (Store rejected / malformed rows)
# -------------------------------------------------------------

spark.sql("""
CREATE TABLE IF NOT EXISTS DQ_QUARANTINE_LOGS (
  quarantine_id       STRING,
  domain              STRING,
  table_name          STRING,
  source_file         STRING,
  record_identifier   STRING,
  reason_code         STRING,
  reason_description  STRING,
  raw_record          STRING,
  load_ts_utc         TIMESTAMP,
  processed_ind       BOOLEAN
) USING DELTA;
""")

print("✅ Created DQ_QUARANTINE_LOGS table")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
