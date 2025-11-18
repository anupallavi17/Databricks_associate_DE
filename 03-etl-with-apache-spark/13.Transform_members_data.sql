-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Transform Memberships Data
-- MAGIC 1. Extract customer_id from the file path 
-- MAGIC 1. Write transformed data to the Silver schema  

-- COMMAND ----------

SELECT * FROM tabular.dataexpert.anallasw_bronze_dea_membership

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1.Extract customer_id from the file path

-- COMMAND ----------

select cast(regexp_extract(path,('.*/([0-9]+)\\.png$'),1) as integer) as customer_id, content as membership_card
from tabular.dataexpert.anallasw_bronze_dea_membership

-- COMMAND ----------

create or replace table tabular.dataexpert.anallasw_silver_membership_dea as
select cast(regexp_extract(path,('.*/([0-9]+)\\.png$'),1) as integer) as customer_id, content as membership_card
from tabular.dataexpert.anallasw_bronze_dea_membership

-- COMMAND ----------

select * from tabular.dataexpert.anallasw_silver_membership_dea

-- COMMAND ----------


