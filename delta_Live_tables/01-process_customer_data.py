# Databricks notebook source
# MAGIC %md
# MAGIC ## Process the Customers Data 
# MAGIC 1. Ingest the data into the data lakehouse - bronze_customers
# MAGIC 1. Perform data quality checks and transform the data as required - silver_customers_clean
# MAGIC 1. Apply changes to the Customers data - silver_customers

# COMMAND ----------

# MAGIC %md
# MAGIC Ingest the data into the data lakehouse - bronze_customers

# COMMAND ----------

# MAGIC %md
# MAGIC CREATE OR REFRESH STREAMING TABLE tabular.dataexpert.anallasw_circuitbox_bronze_customers
# MAGIC COMMENT "raw customers data ingested from source system"
# MAGIC TBLPROPERTIES ('quality' = 'bronze')
# MAGIC AS 
# MAGIC SELECT * ,
# MAGIC   _metadata.file_path AS input_file_path,
# MAGIC   CURRENT_TIMESTAMP() AS ingestion_timestamp
# MAGIC FROM cloud_files("/Volumes/tabular/dataexpert/anallasw/circuitbox/landing/operational_data/customers/",'json', 
# MAGIC MAP('cloudfiles.infercolumnTypes' , 'true') 
# MAGIC );

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING TABLE anallasw_circuitbox_silver_customers_clean(
# MAGIC   CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
# MAGIC   CONSTRAINT valid_customer_name EXPECT (customer_name IS NOT NULL) ON VIOLATION DROP ROW,
# MAGIC   CONSTRAINT valid_telephone EXPECT (length(telephone) >= 10 ),
# MAGIC   CONSTRAINT valid_email EXPECT (email IS NOT NULL),
# MAGIC   CONSTRAINT valid_date_of_birth EXPECT (date_of_birth  >='1920-01-01')
# MAGIC )
# MAGIC COMMENT "data quality check for the data to be stored in silver layer"
# MAGIC TBLPROPERTIES ('quality' = 'silver')
# MAGIC AS 
# MAGIC SELECT customer_id,
# MAGIC customer_name,
# MAGIC CAST(date_of_birth AS DATE) AS date_of_birth,
# MAGIC telephone,
# MAGIC email,
# MAGIC CAST(created_date AS DATE) AS created_date
# MAGIC FROM STREAM(LIVE.anallasw_circuitbox_bronze_customers)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Apply changes to the Customers data - silver_customers

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING TABLE anallasw_circuitbox_silver_customers
# MAGIC COMMENT 'SCD type 1 customers data'
# MAGIC TBLPROPERTIES ('quality' = 'silver')

# COMMAND ----------

# MAGIC %sql
# MAGIC APPLY CHANGES INTO Live.anallasw_circuitbox_silver_customers
# MAGIC FROM STREAM(LIVE.anallasw_circuitbox_silver_customers_clean)
# MAGIC KEYS(customer_id)
# MAGIC SEQUENCE BY created_date
# MAGIC STORED AS SCD type 1;
