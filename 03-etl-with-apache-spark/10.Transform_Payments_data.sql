-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Transform Payments Data
-- MAGIC 1. Extract Date and Time from payment_timestamp and create new columns payment_date and payment_time
-- MAGIC 1. Map payment_status to contain descriptive values  
-- MAGIC    (1-Success, 2-Pending, 3-Cancelled, 4-Failed)
-- MAGIC 1. Write transformed data to the Silver schema 

-- COMMAND ----------

select *,
CAST(date_format(payment_timestamp,'yyyy-MM-dd')as DATE)  as payment_date, 
date_format(payment_timestamp,'HH:mm:ss') as payment_time
from tabular.dataexpert.anallasw_bronze_payments_dea;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Map payment_status to contain descriptive values  
-- MAGIC    (1-Success, 2-Pending, 3-Cancelled, 4-Failed)

-- COMMAND ----------

select payment_id,
order_id,
CAST(date_format(payment_timestamp,'yyyy-MM-dd')as DATE)  as payment_date, 
date_format(payment_timestamp,'HH:mm:ss') as payment_time,
CASE
WHEN payment_status = 1 THEN 'success' 
WHEN payment_status = 2 THEN 'pending'
WHEN payment_status = 3 THEN 'cancelled'
WHEN payment_status = 4 THEN 'Failed'
END as payment_status,
payment_method
from tabular.dataexpert.anallasw_bronze_payments_dea;

-- COMMAND ----------

CREATE TABLE tabular.dataexpert.anallasw_silver_payments_dea
as 
select payment_id,
order_id,
CAST(date_format(payment_timestamp,'yyyy-MM-dd')as DATE)  as payment_date, 
date_format(payment_timestamp,'HH:mm:ss') as payment_time,
CASE
WHEN payment_status = 1 THEN 'success' 
WHEN payment_status = 2 THEN 'pending'
WHEN payment_status = 3 THEN 'cancelled'
WHEN payment_status = 4 THEN 'Failed'
END as payment_status,
payment_method
from tabular.dataexpert.anallasw_bronze_payments_dea;

-- COMMAND ----------

select * from tabular.dataexpert.anallasw_silver_payments_dea

-- COMMAND ----------


