-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Transform Orders Data - Explode Arrays
-- MAGIC 1. Access elements from the JSON object
-- MAGIC 1. Deduplicate Array Elements
-- MAGIC 1. Explode Arrays
-- MAGIC 1. Write the Transformed Data to Silver Schema
-- MAGIC 1. Deduplicate Array Elements
-- MAGIC 1. Explode Arrays
-- MAGIC 1. Write the Transformed Data to Silver Schema

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Access elements from the JSON object

-- COMMAND ----------

select * from tabular.dataexpert.anallasw_silver_orders_json_dea

-- COMMAND ----------

select 
json_value.* 
from tabular.dataexpert.anallasw_silver_orders_json_dea order by json_value.order_id

-- COMMAND ----------

 SELECT json_value.order_id,
        json_value.order_status,
        json_value.payment_method,
        json_value.total_amount,
        json_value.transaction_timestamp,
        json_value.customer_id,
        explode(array_distinct(json_value.items)) as items
  from tabular.dataexpert.anallasw_silver_orders_json_dea order by json_value.order_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3.Explode

-- COMMAND ----------



-- COMMAND ----------

 create or replace temp view orders_exploded_tv
 as
 SELECT json_value.order_id,
        json_value.order_status,
        json_value.payment_method,
        json_value.total_amount,
        json_value.transaction_timestamp,
        json_value.customer_id,
        explode(array_distinct(json_value.items)) as items
  from tabular.dataexpert.anallasw_silver_orders_json_dea order by json_value.order_id

-- COMMAND ----------

SELECT order_id,
       order_status,
       payment_method,
       total_amount,
       cast(transaction_timestamp as timestamp) as transaction_timestamp,
       customer_id,
       items.category,
       items.item_id,
       items.name,
       items.price,
       items.quantity,
       items.details.brand,
       items.details.color
FROM orders_exploded_tv

-- COMMAND ----------

drop table tabular.dataexpert.anallasw_silver_orders_dea

-- COMMAND ----------

create table if not exists tabular.dataexpert.anallasw_silver_orders_dea
as
SELECT order_id,
       order_status,
       payment_method,
       total_amount,
       cast(transaction_timestamp as timestamp) as transaction_timestamp,
       customer_id,
       items.category,
       items.item_id,
       items.name,
       items.price,
       items.quantity,
       items.details.brand,
       items.details.color
FROM orders_exploded_tv

-- COMMAND ----------

select * from tabular.dataexpert.anallasw_silver_orders_dea

-- COMMAND ----------


