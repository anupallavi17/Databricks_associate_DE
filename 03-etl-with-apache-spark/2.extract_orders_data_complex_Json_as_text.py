# Databricks notebook source
# MAGIC %md
# MAGIC ### Extract Data From the Orders complex JSON as text File
# MAGIC - Query Orders File using JSON Format
# MAGIC - Query Orders File using TEXT Format
# MAGIC - Create Orders View in Bronze Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from json.`/Volumes/tabular/dataexpert/anallasw/project_gizmobox/landing/operational_data/orders/`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from text.`/Volumes/tabular/dataexpert/anallasw/project_gizmobox/landing/operational_data/orders/`

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view tabular.dataexpert.bronze_anallasw_dea_orders as
# MAGIC select * from text.`/Volumes/tabular/dataexpert/anallasw/project_gizmobox/landing/operational_data/orders/`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tabular.dataexpert.bronze_anallasw_dea_orders
