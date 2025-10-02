# Databricks notebook source
# MAGIC %md
# MAGIC ### COPY INTO Command
# MAGIC
# MAGIC > - Incrementally loads data into Delta Lake tables from Cloud Storage  
# MAGIC > - Supports schema evolution  
# MAGIC > - Supports wide range of file formats (CSV, JSON, Parquet, Delta)  
# MAGIC > - Alternative to Auto Loader for batch ingestion  
# MAGIC
# MAGIC Documentation - [COPY INTO](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/delta-copy-into)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the table to copy the data into

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists tabular.dataexpert.raw_stock_prices_anallasw

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tabular.dataexpert.raw_stock_prices_anallasw;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Incrementally load new files into the table

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from tabular.dataexpert.raw_stock_prices_anallasw;
# MAGIC COPY INTO tabular.dataexpert.raw_stock_prices_anallasw
# MAGIC from '/Volumes/tabular/dataexpert/anallasw/project_gizmobox/landing/stock_prices/'
# MAGIC FILEFORMAT = JSON
# MAGIC FORMAT_OPTIONS('inferSchema' = 'true')
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tabular.dataexpert.raw_stock_prices_anallasw;

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO tabular.dataexpert.raw_stock_prices_anallasw
# MAGIC from '/Volumes/tabular/dataexpert/anallasw/project_gizmobox/landing/stock_prices/'
# MAGIC FILEFORMAT = JSON
# MAGIC FORMAT_OPTIONS('inferSchema' = 'true')
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tabular.dataexpert.raw_stock_prices_anallasw;

# COMMAND ----------

# MAGIC %md
# MAGIC ### MERGE Statement
# MAGIC > - Used for upserts (Insert/ Update/ Delete operations in a single statement)
# MAGIC > - Allows merging new data into a target table based on matching condition
# MAGIC
# MAGIC Documentation - [MERGE INTO](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/delta-merge-into)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE tabular.dataexpert.stock_prices_anallasw
# MAGIC (
# MAGIC   stock_id STRING,
# MAGIC   price DOUBLE,
# MAGIC   trading_date DATE
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tabular.dataexpert.raw_stock_prices_anallasw

# COMMAND ----------

# MAGIC %md
# MAGIC If we add day-3 data there will duplicated data added through copy into so using delete from command delete the data and add the new data.

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM tabular.dataexpert.raw_stock_prices_anallasw;
# MAGIC     
# MAGIC SELECT * FROM tabular.dataexpert.raw_stock_prices_anallasw;
# MAGIC COPY INTO tabular.dataexpert.raw_stock_prices_anallasw
# MAGIC from '/Volumes/tabular/dataexpert/anallasw/project_gizmobox/landing/stock_prices/'
# MAGIC FILEFORMAT = JSON
# MAGIC FORMAT_OPTIONS('inferSchema' = 'true')
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM tabular.dataexpert.raw_stock_prices_anallasw;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO tabular.dataexpert.stock_prices_anallasw AS TARGET
# MAGIC USING (
# MAGIC   SELECT * FROM tabular.dataexpert.raw_stock_prices_anallasw AS SOURCE
# MAGIC ) ON TARGET.stock_id = SOURCE.stock_id
# MAGIC WHEN MATCHED AND SOURCE.status = 'ACTIVE' THEN 
# MAGIC UPDATE SET 
# MAGIC   TARGET.price = SOURCE.price,
# MAGIC   TARGET.trading_date = SOURCE.trading_date
# MAGIC WHEN MATCHED AND SOURCE.status = 'DELISTED' THEN DELETE
# MAGIC WHEN NOT MATCHED AND SOURCE.status = 'ACTIVE' THEN 
# MAGIC INSERT (stock_id, price, trading_date) VALUES (source.stock_id, source.price, source.trading_date);    
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tabular.dataexpert.stock_prices_anallasw;

# COMMAND ----------


