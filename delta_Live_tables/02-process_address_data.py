# Databricks notebook source

import dlt
import pyspark.sql.functions as F

# COMMAND ----------


@dlt.table(
  name = 'bronze_addresses',
  table_properties = {'quality': 'bronze'},
  comment = 'Raw data ingested to the bronze table'
)
def create_bronze_addresses():
    return (
        spark.readStream
        .format('cloudFiles')
        .option('cloudFiles.format', 'json')
        .option('cloudFiles.infercolumnTypes', 'true')
        .load('/Volumes/tabular/dataexpert/anallasw/circuitbox/landing/operational_data/addresses/')
        .select("*",
                F.col("_metadata.file_path").alias("input_file_path"),
                F.current_timestamp().alias("ingest_timestamp")
                )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC CREATE OR REFRESH STREAMING TABLE tabular.dataexpert.anallasw_circuitbox_bronze_address 
# MAGIC COMMENT "Raw data of address from source system"
# MAGIC TBLPROPERTIES ('quality' =  'bronze')
# MAGIC AS
# MAGIC SELECT *,
# MAGIC  _metadata.file_path AS input_file_path,
# MAGIC   CURRENT_TIMESTAMP() AS ingestion_timestamp 
# MAGIC FROM cloud_files('/Volumes/tabular/dataexpert/anallasw/circuitbox/landing/operational_data/addresses/','json',
# MAGIC map('cloudFiles.inferColumnTypes', 'true'));
# MAGIC

# COMMAND ----------

@dlt.table(
  name = 'silver_addresses_clean',
  table_properties = {'quality': 'silver'}
  )
def create_silver_addresses_clean():
    return (
        spark.readStream.table(
    )
