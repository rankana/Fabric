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
# Bronze Environment Setup Notebook
# -------------------------------------------------------------
# Purpose : One-time setup for Bronze metadata/control table
# Frequency : Run once per environment (Dev/Test/Prod)
# -------------------------------------------------------------

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

print("🔧 Starting Bronze environment setup...")

# -------------------------------------------------------------
# 1️. CONFIG_BRONZE_SOURCES  (Control table for ingestion)
# -------------------------------------------------------------

spark.sql("""
CREATE TABLE IF NOT EXISTS CONFIG_BRONZE_SOURCES (
  domain              STRING,
  table_name          STRING,
  target_table        STRING,
  schema_path         STRING,
  input_glob          STRING,
  delimiter           STRING,
  has_header          BOOLEAN,
  source_system       STRING,
  business_key        STRING,
  write_mode          STRING,
  enabled             BOOLEAN,
  priority            INT,
  max_files_per_run   INT,
  parse_options       STRING,
  dedupe_keys         STRING,
  checkpoint_path     STRING,
  created_ts_utc      TIMESTAMP,
  updated_ts_utc      TIMESTAMP
) USING DELTA;
""")

print("✅ Created CONFIG_BRONZE_SOURCES table")


# -------------------------------------------------------------
# 2. Populate CONFIG_BRONZE_SOURCES with initial setup rows
# -------------------------------------------------------------

spark.sql("SET NOW_UTC = current_timestamp();")

spark.sql("""
MERGE INTO CONFIG_BRONZE_SOURCES AS target
USING (
  SELECT
    col1  AS domain,
    col2  AS table_name,
    col3  AS target_table,
    col4  AS schema_path,
    col5  AS input_glob,
    col6  AS delimiter,
    col7  AS has_header,
    col8  AS source_system,
    col9  AS business_key,
    col10 AS write_mode,
    col11 AS enabled,
    col12 AS priority,
    col13 AS max_files_per_run,
    col14 AS parse_options,
    col15 AS dedupe_keys,
    col16 AS checkpoint_path,
    col17 AS created_ts_utc,
    col18 AS updated_ts_utc
  FROM (
    SELECT * FROM VALUES
    -- ==========================================================
-- Reference (REF)
-- ==========================================================
('REF','REF_CODEVALUE_LOCAL','BRZ_REF_CODEVALUE_LOCAL',
 'Files/_metadata/schema_registry/REF/REF_CODEVALUE_LOCAL_V1.json',
 'Files/raw_files/reference_data/REF_CODEVALUE_LOCAL/y=*/m=*/d=*/*.csv',
 '|', true, 'OracleHealth',
 '["CODE_SET_NBR","CODE_VALUE"]',
 'append', true, 10, 2000, NULL, NULL, NULL, ${NOW_UTC}, ${NOW_UTC}),

('REF','REF_CODEVALUE_NATIONAL','BRZ_REF_CODEVALUE_NATIONAL',
 'Files/_metadata/schema_registry/REF/REF_CODEVALUE_NATIONAL_V1.json',
 'Files/raw_files/reference_data/REF_CODEVALUE_NATIONAL/y=*/m=*/d=*/*.csv',
 '|', true, 'OracleHealth',
 '["LOCAL_CODE_SET_NBR","LOCAL_CODE"]',
 'append', true, 11, 2000, NULL, NULL, NULL, ${NOW_UTC}, ${NOW_UTC}),

('REF','REF_CODE_MAPPING','BRZ_REF_CODE_MAPPING',
 'Files/_metadata/schema_registry/REF/REF_CODE_MAPPING_V1.json',
 'Files/raw_files/reference_data/REF_CODE_MAPPING/y=*/m=*/d=*/*.csv',
 '|', true, 'OracleHealth',
 '["LOCAL_CODE","NATIONAL_CODE"]',
 'append', true, 12, 2000, NULL, NULL, NULL, ${NOW_UTC}, ${NOW_UTC}),
 -- ==========================================================
-- Person Demographics (PDG)
-- ==========================================================
('PDG','PDG_PERSON_CORE','BRZ_PDG_PERSON_CORE',
 'Files/_metadata/schema_registry/PDG/PDG_PERSON_CORE_V1.json',
 'Files/raw_files/pdg/PDG_PERSON_CORE/y=*/m=*/d=*/*.csv',
 '|', true, 'OracleHealth',
 '["PERSON_ID"]',
 'append', true, 20, 2000, NULL, NULL, NULL, ${NOW_UTC}, ${NOW_UTC}),

('PDG','PDG_PERSON_CURRENT','BRZ_PDG_PERSON_CURRENT',
 'Files/_metadata/schema_registry/PDG/PDG_PERSON_CURRENT_V1.json',
 'Files/raw_files/pdg/PDG_PERSON_CURRENT/y=*/m=*/d=*/*.csv',
 '|', true, 'OracleHealth',
 '["PERSON_ID"]',
 'append', true, 21, 2000, NULL, NULL, NULL, ${NOW_UTC}, ${NOW_UTC}),

('PDG','PDG_PERSON_NAME','BRZ_PDG_PERSON_NAME',
 'Files/_metadata/schema_registry/PDG/PDG_PERSON_NAME_V1.json',
 'Files/raw_files/pdg/PDG_PERSON_NAME/y=*/m=*/d=*/*.csv',
 '|', true, 'OracleHealth',
 '["PERSON_ID","NAME_TYPE_CODE","EFFECTIVE_FROM"]',
 'append', true, 22, 2000, NULL, NULL, NULL, ${NOW_UTC}, ${NOW_UTC}),

('PDG','PDG_ADDRESS','BRZ_PDG_ADDRESS',
 'Files/_metadata/schema_registry/PDG/PDG_ADDRESS_V1.json',
 'Files/raw_files/pdg/PDG_ADDRESS/y=*/m=*/d=*/*.csv',
 '|', true, 'OracleHealth',
 '["PERSON_ID","EFFECTIVE_FROM"]',
 'append', true, 23, 2000, NULL, NULL, NULL, ${NOW_UTC}, ${NOW_UTC}),

('PDG','PDG_PHONE','BRZ_PDG_PHONE',
 'Files/_metadata/schema_registry/PDG/PDG_PHONE_V1.json',
 'Files/raw_files/pdg/PDG_PHONE/y=*/m=*/d=*/*.csv',
 '|', true, 'OracleHealth',
 '["PERSON_ID","PHONE_TYPE_CODE","EFFECTIVE_FROM"]',
 'append', true, 24, 2000, NULL, NULL, NULL, ${NOW_UTC}, ${NOW_UTC}),

-- ==========================================================
-- Outpatient (OPA)
-- ==========================================================
('OPA','OPA_APPT','BRZ_OPA_APPT',
 'Files/_metadata/schema_registry/OPA/OPA_APPT_V1.json',
 'Files/raw_files/opa/OPA_APPT/y=*/m=*/d=*/*.csv',
 '|', true, 'OracleHealth',
 '["APPT_ID"]',
 'append', true, 30, 4000, NULL, NULL, NULL, ${NOW_UTC}, ${NOW_UTC}),

('OPA','OPA_DIAGNOSES','BRZ_OPA_DIAGNOSES',
 'Files/_metadata/schema_registry/OPA/OPA_DIAGNOSES_V1.json',
 'Files/raw_files/opa/OPA_DIAGNOSES/y=*/m=*/d=*/*.csv',
 '|', true, 'OracleHealth',
 '["APPT_ID","DIAGNOSIS_SEQ"]',
 'append', true, 31, 4000, NULL, NULL, NULL, ${NOW_UTC}, ${NOW_UTC}),

('OPA','OPA_PROCEDURES','BRZ_OPA_PROCEDURES',
 'Files/_metadata/schema_registry/OPA/OPA_PROCEDURES_V1.json',
 'Files/raw_files/opa/OPA_PROCEDURES/y=*/m=*/d=*/*.csv',
 '|', true, 'OracleHealth',
 '["APPT_ID","PROCEDURE_SEQ"]',
 'append', true, 32, 4000, NULL, NULL, NULL, ${NOW_UTC}, ${NOW_UTC}),

('OPA','OPA_REFERRALS','BRZ_OPA_REFERRALS',
 'Files/_metadata/schema_registry/OPA/OPA_REFERRALS_V1.json',
 'Files/raw_files/opa/OPA_REFERRALS/y=*/m=*/d=*/*.csv',
 '|', true, 'OracleHealth',
 '["REFERRAL_ID"]',
 'append', true, 33, 4000, NULL, NULL, NULL, ${NOW_UTC}, ${NOW_UTC}),

('OPA','OPA_SCHEDULEDETAILS','BRZ_OPA_SCHEDULEDETAILS',
 'Files/_metadata/schema_registry/OPA/OPA_SCHEDULEDETAILS_V1.json',
 'Files/raw_files/opa/OPA_SCHEDULEDETAILS/y=*/m=*/d=*/*.csv',
 '|', true, 'OracleHealth',
 '["APPT_ID","SLOT_TYPE_CODE","SCHEDULED_START_TS"]',
 'append', true, 34, 4000, NULL, NULL, NULL, ${NOW_UTC}, ${NOW_UTC})
  ) AS tmp
) AS src
ON target.domain = src.domain AND target.table_name = src.table_name
WHEN MATCHED THEN
  UPDATE SET
    target.updated_ts_utc     = current_timestamp(),
    target.target_table       = src.target_table,
    target.schema_path        = src.schema_path,
    target.input_glob         = src.input_glob,
    target.delimiter          = src.delimiter,
    target.has_header         = src.has_header,
    target.source_system      = src.source_system,
    target.business_key       = src.business_key,
    target.write_mode         = src.write_mode,
    target.enabled            = src.enabled,
    target.priority           = src.priority,
    target.max_files_per_run  = src.max_files_per_run,
    target.parse_options      = src.parse_options,
    target.dedupe_keys        = src.dedupe_keys,
    target.checkpoint_path    = src.checkpoint_path
WHEN NOT MATCHED THEN
  INSERT (
    domain, table_name, target_table, schema_path, input_glob, delimiter,
    has_header, source_system, business_key, write_mode, enabled, priority,
    max_files_per_run, parse_options, dedupe_keys, checkpoint_path,
    created_ts_utc, updated_ts_utc
  )
  VALUES (
    src.domain, src.table_name, src.target_table, src.schema_path, src.input_glob, src.delimiter,
    src.has_header, src.source_system, src.business_key, src.write_mode, src.enabled, src.priority,
    src.max_files_per_run, src.parse_options, src.dedupe_keys, src.checkpoint_path,
    src.created_ts_utc, src.updated_ts_utc
  );
""");


print("✅ Seeded CONFIG_BRONZE_SOURCES with base rows")

# -------------------------------------------------------------
# Done
# -------------------------------------------------------------
print("🎉 Bronze environment setup complete.")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM CONFIG_BRONZE_SOURCES

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
