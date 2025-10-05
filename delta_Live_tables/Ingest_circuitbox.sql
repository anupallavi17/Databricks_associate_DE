-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Process the Customers Data 
-- MAGIC 1. Ingest the data into the data lakehouse - bronze_customers
-- MAGIC 1. Perform data quality checks and transform the data as required - silver_customers_clean
-- MAGIC 1. Apply changes to the Customers data - silver_customers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Ingest the data into the data lakehouse - bronze_customers

-- COMMAND ----------


CREATE OR REFRESH STREAMING LIVE TABLE tabular.dataexpert.anallasw_circuitbox_bronze_customers
COMMENT "raw customers data ingested from source system"
TBLPROPERTIES ('quality' = 'bronze')
AS 
SELECT * ,
  _metadata.file_path AS input_file_path,
  CURRENT_TIMESTAMP() AS ingestion_timestamp
FROM cloud_files("/Volumes/tabular/dataexpert/anallasw/circuitbox/landing/operational_data/customers/",'json', 
MAP('cloudfiles.infercolumnTypes' , 'true') 
);

-- COMMAND ----------


