# Databricks notebook source
# MAGIC %md
# MAGIC ### OPTIMIZE Command
# MAGIC
# MAGIC > Merges multiple small files into fewer, larger files, thus improves the performance

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Create table - demo.delta_lake.optimize_stock_prices

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tabular.dataexpert.optimize_stock_prices_anallasw;
# MAGIC     
# MAGIC CREATE TABLE IF NOT EXISTS  tabular.dataexpert.optimize_stock_prices_anallasw
# MAGIC (
# MAGIC   stock_id STRING,
# MAGIC   price DOUBLE,
# MAGIC   trading_date DATE
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO tabular.dataexpert.optimize_stock_prices_anallasw
# MAGIC VALUES ('AAPL', 227.65, "2025-02-10");

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO tabular.dataexpert.optimize_stock_prices_anallasw
# MAGIC VALUES ('GOOGL', 2775.0, "2025-02-10");

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO tabular.dataexpert.optimize_stock_prices_anallasw
# MAGIC VALUES ('MSFT', 325.0, "2025-02-10");

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO tabular.dataexpert.optimize_stock_prices_anallasw
# MAGIC VALUES ('AMZN', 3520.0, "2025-02-12");

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tabular.dataexpert.optimize_stock_prices_anallasw;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY tabular.dataexpert.optimize_stock_prices_anallasw;

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Check the number of files required to get the latest data

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC DETAIL tabular.dataexpert.optimize_stock_prices_anallasw;

# COMMAND ----------

# MAGIC %md
# MAGIC the small files are not deleted.A new large file is created and hence the small files can be seen using version sommand in history

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tabular.dataexpert.optimize_stock_prices_anallasw VERSION AS OF 3;

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Run OPTIMIZE

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE tabular.dataexpert.optimize_stock_prices_anallasw;

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE tabular.dataexpert.optimize_stock_prices_anallasw ZORDER BY (stock_id)

# COMMAND ----------


