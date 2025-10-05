# Databricks notebook source
# MAGIC %md
# MAGIC create an external location for the datalake -> container in Azure.This needs a storage control to connsct with the unity catalog in Databricks. Here we are uploading directly to volume and doing transformations.
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS <external_location_name> 
# MAGIC URL 'abfss://circuitbox@datalake_name.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIALS name_of_sc)
# MAGIC COMMET 'External location for circuitbox data lakehouse' 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC create a catalog for circuitbox project but here I am using tabular catalog and I have dataexpert schema.
# MAGIC Ideally we can create catalog using ,
# MAGIC CREATE CATALOG IF NOT EXISTS circuitbox
# MAGIC MANAGED LOCATION 'abfss://circuitbox@datalake_name.dfs.core.windows.net/' 
# MAGIC COMMENT 'catalog for circuitbox lakehouse'

# COMMAND ----------

# MAGIC %md
# MAGIC USE CATALOG circuitbox
# MAGIC create schema landing and lakehouse using commands
# MAGIC CREATE SCHEMA IF NOT EXISTS Landing
# MAGIC MANAGED LOCATION 'abfss://circuitbox@datalake_name.dfs.core.windows.net/landing'
# MAGIC COMMENT 'schema for circuitbox landing' 
# MAGIC CREATE SCHEMA IF NOT EXISTS Lakehouse
# MAGIC MANAGED LOCATION 'abfss://circuitbox@datalake_name.dfs.core.windows.net/lakehouse'
# MAGIC COMMENT 'schema for circuitbox Lakehouse' 

# COMMAND ----------

# MAGIC %md
# MAGIC USE CATALOG tabular;
# MAGIC USE SCHEMA dataexpert;
# MAGIC CREATE EXTERNAL VOLUME IF NOT EXISTS operational_data
# MAGIC LOCATION 'abfss://circuitbox@datalake_name.dfs.core.windows.net/landing/operational_data/'

# COMMAND ----------

# MAGIC %fs ls /Volumes/tabular/dataexpert/anallasw/circuitbox/landing/operational_data

# COMMAND ----------


