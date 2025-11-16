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
# Bronze Bootstrap Tables
# Purpose : Auto-create BRZ_* Delta tables from schema registry
# Frequency : Run once (or re-run when new schemas added)
# -------------------------------------------------------------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType, BooleanType

spark = spark

print("🔧 Starting Bronze table bootstrap...")

# -------------------------------------------------------------------
# Helper : Convert JSON type string to Spark type
# -------------------------------------------------------------------
def get_spark_type(dtype):
    mapping = {
        "STRING": StringType(),
        "INT": IntegerType(),
        "BIGINT": IntegerType(),
        "DOUBLE": DoubleType(),
        "FLOAT": DoubleType(),
        "BOOLEAN": BooleanType(),
        "DATE": DateType(),
        "TIMESTAMP": TimestampType()
    }
    return mapping.get(dtype.upper(), StringType())

# -------------------------------------------------------------------
# Load all schema definitions from CONFIG_BRONZE_SOURCES
# -------------------------------------------------------------------
config_df = spark.table("CONFIG_BRONZE_SOURCES").filter("enabled = true").select("domain","table_name","target_table","schema_path")

for row in config_df.collect():
    domain       = row["domain"]
    table_name   = row["table_name"]
    target_table = row["target_table"]
    schema_path  = row["schema_path"]

    print(f"🔍 Checking table: {target_table}")

    if spark._jsparkSession.catalog().tableExists(target_table):
        print(f"✅ Exists: {target_table}")
        continue

    # ----------------------------------------------------------------
    # Read schema JSON
    # ----------------------------------------------------------------
    try:
        schema_json = spark.read.text(schema_path).collect()[0][0]
    except Exception as e:
        print(f"❌ Schema load failed for {schema_path}: {e}")
        continue

    import json
    schema_def = json.loads(schema_json)
    data_type_map = schema_def.get("data_type_map", {})
    expected_cols = schema_def.get("expected_columns", [])

    # Build Spark StructType
    struct_fields = []
    for col in expected_cols:
        dtype = data_type_map.get(col, "STRING")
        struct_fields.append(StructField(col, get_spark_type(dtype), True))
    # Add Bronze metadata fields
    bronze_meta = [
        StructField("SOURCE_SYSTEM", StringType(), True),
        StructField("LOAD_RUN_ID", StringType(), True),
        StructField("LOAD_TS_UTC", TimestampType(), True),
        StructField("FILE_NAME", StringType(), True),
        StructField("RECORD_HASH", StringType(), True)
    ]
    full_schema = StructType(struct_fields + bronze_meta)

    # Create empty DataFrame and write as Delta table
    empty_df = spark.createDataFrame([], full_schema)

    empty_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
    print(f"✅ Created table: {target_table}")

print("🎉 Bronze table bootstrap complete.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
