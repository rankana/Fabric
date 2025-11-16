# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from pyspark.sql import functions as F

summary = (spark.table("AUDIT_BRONZE_LOADS")
  .filter("load_start_ts_utc >= current_date()")
  .groupBy("load_status")
  .agg(F.count("*").alias("count"))
  .collect())

print("Bronze summary for today:")
for row in summary:
    print(f"  {row['load_status']}: {row['count']} tables")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
