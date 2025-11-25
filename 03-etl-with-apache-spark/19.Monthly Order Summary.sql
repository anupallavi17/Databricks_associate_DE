-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Monthly Order Summary
-- MAGIC For each of the customer, produce the following summary per month
-- MAGIC 1. total orders
-- MAGIC 1. total items bought
-- MAGIC 1. total amount spent

-- COMMAND ----------

SELECT * FROM tabular.dataexpert.anallasw_silver_orders_dea;

-- COMMAND ----------

SELECT DATE_FORMAT(transaction_timestamp,'yyyy-MM') as order_month,
customer_id,
COUNT(DISTINCT order_id) AS total_orders,
SUM(quantity) as total_items,
SUM(quantity* price) as total_amount
FROM tabular.dataexpert.anallasw_silver_orders_dea
GROUP BY order_month,customer_id
ORDER BY order_month


-- COMMAND ----------

CREATE OR REPLACE TABLE tabular.dataexpert.anallasw_gold_monthly_order_summary_dea
AS
SELECT DATE_FORMAT(transaction_timestamp,'yyyy-MM') as order_month,
customer_id,
COUNT(DISTINCT order_id) AS total_orders,
SUM(quantity) as total_items,
SUM(quantity* price) as total_amount
FROM tabular.dataexpert.anallasw_silver_orders_dea
GROUP BY order_month,customer_id
ORDER BY order_month

-- COMMAND ----------

select * from tabular.dataexpert.anallasw_gold_monthly_order_summary_dea

-- COMMAND ----------


