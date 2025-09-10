# Databricks notebook source
# MAGIC %md
# MAGIC ### 1. Read files using DataStreamReader API

# COMMAND ----------

customer_stream_df = spark.readStream\
                    .format("cloudFiles")\
                    .option("cloudFiles.format", "json")\
                    .option("cloudFiles.schemaLocation", "/Volumes/tabular/dataexpert/anallasw/project_gizmobox/bronze/operational_data/customers_stream/_schema")\
                    .option("cloudFiles.inferColumnTypes", "true")\
                    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")\
                    .option("cloudFiles.schemaHints","date_of_birth DATE,member_since DATE,created_timestamp TIMESTAMP")\
                    .load("/Volumes/tabular/dataexpert/anallasw/project_gizmobox/landing/operational_data/customers_autoloader/")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

customers_transformed_df = (
                                customer_stream_df.withColumn("file_path", col("_metadata.file_path"))
                                            .withColumn("ingestion_date", current_timestamp())
)

# COMMAND ----------

customers_transformed_df.writeStream.format("delta").option("checkpointLocation", "/Volumes/tabular/dataexpert/anallasw/project_gizmobox/bronze/operational_data/customers_autoloader/_checkpoint").toTable("tabular.dataexpert.anallasw_bronze_customers_autoloader_dea")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tabular.dataexpert.anallasw_bronze_customers_autoloader_dea

# COMMAND ----------


