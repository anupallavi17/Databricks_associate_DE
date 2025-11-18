# Databricks notebook source
# MAGIC %md
# MAGIC ls 'abfss://gizmobox@dl.dfs.core.windows.net/landing/external_data/payments'

# COMMAND ----------

# MAGIC %md
# MAGIC create table if not exists tabular.dataexpert.anallasw_bronze_payments_dea 
# MAGIC (payment_id INTEGER,order_id INTEGER, Payment_timestamp TIMESTAMP,payment_status INTEGER, payment_method STRING)
# MAGIC USING CSV
# MAGIC OPTIONS(header = "true",
# MAGIC delimiter = ",")
# MAGIC LOCATION '/Volumes/tabular/dataexpert/anallasw/project_gizmobox/landing/External_data/payments/'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv.`/Volumes/tabular/dataexpert/anallasw/project_gizmobox/landing/External_data/payments/*.csv`

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table tabular.dataexpert.anallasw_bronze_payments_dea_t 
# MAGIC as
# MAGIC SELECT * from read_files('/Volumes/tabular/dataexpert/anallasw/project_gizmobox/landing/External_data/payments/',
# MAGIC format =>"csv",
# MAGIC schema => "payment_id INT, order_id INT, payment_timestamp TIMESTAMP, payment_status INT, payment_method STRING",
# MAGIC delimiter =>",")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tabular.dataexpert.anallasw_bronze_payments_dea_t;

# COMMAND ----------


