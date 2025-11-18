# Databricks notebook source
# MAGIC %sql
# MAGIC select * from csv.`/Volumes/tabular/dataexpert/anallasw/project_gizmobox/landing/operational_data/addresses/`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from read_files('/Volumes/tabular/dataexpert/anallasw/project_gizmobox/landing/operational_data/addresses/',
# MAGIC format=>'csv',
# MAGIC delimiter=>'\t',
# MAGIC inferSchema=>'true',
# MAGIC header=>'true')

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view tabular.dataexpert.anallasw_bronze_address_dea as
# MAGIC select * from read_files('/Volumes/tabular/dataexpert/anallasw/project_gizmobox/landing/operational_data/addresses/',
# MAGIC format => 'csv',
# MAGIC header => 'true',
# MAGIC delimiter => '\t',
# MAGIC inferSchema => 'true')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tabular.dataexpert.anallasw_bronze_address_dea;

# COMMAND ----------


