# Databricks notebook source
# MAGIC %sql
# MAGIC select * from tabular.dataexpert.anallasw_bronze_customer_dea_v;

# COMMAND ----------

df = spark.table('tabular.dataexpert.anallasw_bronze_customer_dea')
dbutils.data.summarize(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC show columns from tabular.dataexpert.anallasw_bronze_customer_dea

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*),count_if (customer_id is NULL) from tabular.dataexpert.anallasw_bronze_customer_dea;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count (DISTINCT customer_id) from tabular.dataexpert.anallasw_bronze_customer_dea where customer_id is not NULL;

# COMMAND ----------


