-- Databricks notebook source
SELECT customer_id,
       address_type,
       address_line_1,
       city,
       state,
       postcode from tabular.dataexpert.anallasw_bronze_address_dea

-- COMMAND ----------

SELECT * FROM
(SELECT customer_id,
       address_type,
       address_line_1,
       city,
       state,
       postcode
       FROM tabular.dataexpert.anallasw_bronze_address_dea)
pivot (
      Max(address_line_1)as address_line_1,
      Max(city) as city,
      Max(state) as state,
      MAX(postcode) as postcode
      FOR address_type IN ('billing','shipping'))


-- COMMAND ----------

CREATE TABLE IF NOT EXISTS tabular.dataexpert.anallasw_silver_address_dea
as
SELECT * FROM
(SELECT customer_id,
       address_type,
       address_line_1,
       city,
       state,
       postcode
       FROM tabular.dataexpert.anallasw_bronze_address_dea)
pivot (
      Max(address_line_1)as address_line_1,
      Max(city) as city,
      Max(state) as state,
      MAX(postcode) as postcode
      FOR address_type IN ('billing','shipping'))

-- COMMAND ----------

select * from tabular.dataexpert.anallasw_silver_address_dea

-- COMMAND ----------


