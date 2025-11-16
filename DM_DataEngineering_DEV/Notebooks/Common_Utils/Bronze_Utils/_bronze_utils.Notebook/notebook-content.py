# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
import uuid, json
from pyspark.sql import functions as F

def add_bronze_metadata(df, source_system, business_cols):
    return (df
        .withColumn("_ingestion_id", F.lit(str(uuid.uuid4())))
        .withColumn("_source_system", F.lit(source_system))
        .withColumn("_file_path", F.input_file_name())
        .withColumn("_file_name", F.element_at(F.split(F.input_file_name(), "/"), -1))
        .withColumn("_load_ts_utc", F.current_timestamp())
        .withColumn("_row_hash_sha256", F.sha2(F.concat_ws("||", *[F.col(c) for c in business_cols]), 256))
        .withColumn("_is_deleted", F.lit(False))
        .withColumn("_year",  F.year(F.col("_load_ts_utc")))
        .withColumn("_month", F.month(F.col("_load_ts_utc")))
        .withColumn("_day",   F.dayofmonth(F.col("_load_ts_utc")))
    )

def load_schema_def(spark, path_json):
    # path_json e.g. "Files/fabric/bronze/config/schemas/REF_CODEVALUE_LOCAL.json"
    js = spark.read.text(path_json).collect()[0][0]
    return json.loads(js)

def normalise_cols(df):
    for c in df.columns:
        df = df.withColumnRenamed(c, c.upper())
    return df

def reconcile_schema(df, expected_cols, soft=True):
    actual = [c.upper() for c in df.columns]
    missing = [c for c in expected_cols if c not in actual]
    extra   = [c for c in actual if c not in expected_cols]

    # Fill missing with NULLs
    for m in missing:
        df = df.withColumn(m, F.lit(None).cast("string"))

    # Optionally capture extras for inspection
    if extra and soft:
        df = df.withColumn("_extra_json", F.to_json(F.struct(*[F.col(x) for x in extra])))
    return df, missing, extra

def write_audit(spark, table, files, rows, quarantined, status, err=None):
    from pyspark.sql import Row
    data = [Row(
        load_id=str(uuid.uuid4()),
        table_name=table,
        files_loaded=files,
        rows_ingested=rows,
        rows_quarantined=quarantined,
        status=status,
        started_ts_utc=F.current_timestamp().cast("string"),
        ended_ts_utc=F.current_timestamp().cast("string"),
        error_message=err
    )]
    spark.createDataFrame(data).write.mode("append").format("delta").saveAsTable("AUDIT_BRONZE_LOADS")

def quarantine(df, table_name):
    df.write.mode("append").format("delta").saveAsTable("DQ_QUARANTINE_BRONZE_" + table_name)


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
