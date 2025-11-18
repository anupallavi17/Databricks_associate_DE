-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Query Orders Data as JSON Strings
-- MAGIC 1. Extract Top Level Column Values
-- MAGIC 1. Extract Array elements
-- MAGIC 1. Extract Nested Column Values
-- MAGIC 1. CAST Column Values to a Specific Data Type

-- COMMAND ----------

select * from tabular.dataexpert.anallasw_dea_v_orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Extract Top Level Column Values

-- COMMAND ----------

select value:order_id as order_id,value from tabular.dataexpert.anallasw_dea_v_orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Extract Array elements

-- COMMAND ----------

select value:items[0] as item_1,value:items[1] as item_2,value from tabular.dataexpert.anallasw_dea_v_orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3.Extract Nested Column Values

-- COMMAND ----------

select value:items[0].item_id::integer as item_id_item_1,
      value:items[0].name as item_name,
      value 
      from tabular.dataexpert.anallasw_dea_v_orders

-- COMMAND ----------


