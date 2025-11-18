# Databricks notebook source
df = spark.sql("SELECT * FROM  json.`/Volumes/tabular/dataexpert/anallasw/project_gizmobox/landing/operational_data/customers/`")
df.display()

# COMMAND ----------

df_1 = spark.sql("CREATE OR REPLACE view  tabular.dataexpert.anallasw_bronze_customer_dea_V AS SELECT * FROM  json.`/Volumes/tabular/dataexpert/anallasw/project_gizmobox/landing/operational_data/customers/`")
display(df_1)

# COMMAND ----------

display(df_1)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM  json.`/Volumes/tabular/dataexpert/anallasw/project_gizmobox/landing/operational_data/customers/`

# COMMAND ----------



# COMMAND ----------


