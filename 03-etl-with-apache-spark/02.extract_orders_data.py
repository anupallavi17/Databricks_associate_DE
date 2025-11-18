# Databricks notebook source
# MAGIC %md
# MAGIC ### ## 1.Query orders file using Json format

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM json.`/Volumes/tabular/dataexpert/anallasw/project_gizmobox/landing/operational_data/orders/`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM text.`/Volumes/tabular/dataexpert/anallasw/project_gizmobox/landing/operational_data/orders/`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW tabular.dataexpert.anallasw_dea_v_orders
# MAGIC AS
# MAGIC SELECT * FROM text.`/Volumes/tabular/dataexpert/anallasw/project_gizmobox/landing/operational_data/orders/`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tabular.dataexpert.anallasw_dea_v_orders

# COMMAND ----------


