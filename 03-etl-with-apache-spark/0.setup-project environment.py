# Databricks notebook source
# MAGIC %md
# MAGIC Access Gizmobox container from databricks.As there are no connection established we will not be able to access the container.Create a external container to access the container in Azure storage

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create external storage container

# COMMAND ----------

# MAGIC %md
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS <cloud_storage_DATALAKE_CONTAINER_name> 
# MAGIC URL 'abfss://container@dl<EMAIL>.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL dae_course_ext_sc)
# MAGIC COMMENT 'External locationfor the gizmobox Data lakehouse'

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_catalog();

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG tabular;

# COMMAND ----------

# MAGIC %sql
# MAGIC show catalogs;

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_catalog();

# COMMAND ----------

# MAGIC %md
# MAGIC - use catalog tabular;
# MAGIC - create schema if not exists landing
# MAGIC - COMMENT 'creating schema for landing data'
# MAGIC - MANAGED LOCATION 'abfss://container@dl.dfs.core.windows.net/landing'
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC similarly create schema for bronze, silver , gold

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. create external volume inside the landing schema 

# COMMAND ----------

# MAGIC %md
# MAGIC create external volume IF NOT EXISTS operational_data
# MAGIC Location <'abfss://container@dl.dfs.core.windows.net/landing/opeational_data'>
