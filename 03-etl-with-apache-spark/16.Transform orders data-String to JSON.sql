-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Transform Orders Data - String to JSON
-- MAGIC 1. Pre-process the JSON String to fix the Data Quality Issues
-- MAGIC 1. Transform JSON String to JSON Object
-- MAGIC 1. Write transformed data to the silver schema

-- COMMAND ----------

select * from tabular.dataexpert.anallasw_dea_v_orders

-- COMMAND ----------

create or replace temp view anallasw_bronze_orders_tv
as
SELECT value,
       regexp_replace(value, '"order_date": (\\d{4}-\\d{2}-\\d{2})', '"order_date": "\$1"') AS fixed_value 
  FROM tabular.dataexpert.anallasw_dea_v_orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2.Transform JSON String to JSON Object

-- COMMAND ----------

SELECT schema_of_json(fixed_value), fixed_value from anallasw_bronze_orders_tv
LIMIT 1

-- COMMAND ----------

SELECT from_json(fixed_value,'STRUCT<customer_id: BIGINT, items: ARRAY<STRUCT<category: STRING, details: STRUCT<brand: STRING, color: STRING>, item_id: BIGINT, name: STRING, price: BIGINT, quantity: BIGINT>>, order_date: STRING, order_id: BIGINT, order_status: STRING, payment_method: STRING, total_amount: BIGINT, transaction_timestamp: STRING>') AS json_value,
fixed_value from anallasw_bronze_orders_tv

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS tabular.dataexpert.anallasw_silver_orders_json_dea 
AS
  SELECT from_json(fixed_value,
                  'STRUCT<customer_id: BIGINT, items: ARRAY<STRUCT<category: STRING, details: STRUCT<brand: STRING, color:   STRING>, item_id: BIGINT, name: STRING, price: BIGINT, quantity: BIGINT>>, order_date: STRING, order_id: BIGINT, order_status: STRING, payment_method: STRING, total_amount: BIGINT, transaction_timestamp: STRING>') AS json_value
  FROM anallasw_bronze_orders_tv

-- COMMAND ----------

select * from tabular.dataexpert.anallasw_silver_orders_json_dea

-- COMMAND ----------


