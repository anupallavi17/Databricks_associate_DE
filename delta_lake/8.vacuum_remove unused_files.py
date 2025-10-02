# Databricks notebook source
# MAGIC %md
# MAGIC # Remove unused files using VACUUM

# COMMAND ----------

# MAGIC %md
# MAGIC ### VACUUM Command
# MAGIC
# MAGIC > Used to remove old, unused files from the Delta Lake to free up storage. Permanently deletes files that are no longer referenced in the transaction log and older than retention threshold (default 7 days)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Check the table history

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY tabular.dataexpert.optimize_stock_prices_anallasw;

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM tabular.dataexpert.optimize_stock_prices_anallasw;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY tabular.dataexpert.optimize_stock_prices_anallasw;
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC to set the retention duration to 0 hours will delete the file but it is not recommended(assigning to 0) in production.
# MAGIC This will not work in serverless compute as we can not update the config directly.

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled=false
# MAGIC VACUUM tabular.dataexpert.optimize_stock_prices_anallasw RETAIN 0 HOURS;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC The below command will error as there will be no parquet file to refer

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tabular.dataexpert.optimize_stock_prices_anallasw VERSION AS OF 3;  
