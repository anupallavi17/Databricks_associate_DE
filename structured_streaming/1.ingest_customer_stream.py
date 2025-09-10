# Databricks notebook source
# MAGIC %md
# MAGIC ### 1. Read files using DataStreamReader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType

customers_schema = StructType(fields=[StructField("customer_id", IntegerType()),
                                     StructField("customer_name", StringType()),
                                     StructField("date_of_birth", DateType()),
                                     StructField("telephone", StringType()),
                                     StructField("email", StringType()),
                                     StructField("member_since", DateType()),
                                     StructField("created_timestamp", TimestampType())
                                    ]
)

# COMMAND ----------

customer_stream_df = spark.readStream.format("json").schema(customers_schema).load("/Volumes/tabular/dataexpert/anallasw/project_gizmobox/landing/operational_data/customers_stream/")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

customers_transformed_df = (
                                customer_stream_df.withColumn("file_path", col("_metadata.file_path"))
                                            .withColumn("ingestion_date", current_timestamp())
)

# COMMAND ----------

customers_transformed_df.writeStream.format("delta").outputMode("append").option("checkpointLocation", "/Volumes/tabular/dataexpert/anallasw/project_gizmobox/bronze/operational_data/customers_stream/_checkpoint").toTable("tabular.dataexpert.anallasw_bronze_customers_stream_dea")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tabular.dataexpert.anallasw_bronze_customers_stream_dea

# COMMAND ----------


