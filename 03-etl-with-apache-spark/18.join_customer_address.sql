-- Databricks notebook source
select * from tabular.dataexpert.anallasw_silver_customer_dea

-- COMMAND ----------

select * from tabular.dataexpert.anallasw_silver_address_dea

-- COMMAND ----------

CREATE OR REPLACE TABLE tabular.dataexpert.anallasw_gold_customer_address_dea
AS 
SELECT c.customer_id,
       c.customer_name,
       c.email,
       c.date_of_birth,
       c.member_since,
       c.telephone,
       a.shipping_address_line_1,
       a.shipping_city,
       a.shipping_state,
       a.shipping_postcode,
       a.billing_address_line_1,
       a.billing_city,
       a.billing_state,
       a.billing_postcode
FROM tabular.dataexpert.anallasw_silver_customer_dea c
JOIN tabular.dataexpert.anallasw_silver_address_dea a 
ON c.customer_id = a.customer_id


-- COMMAND ----------

SELECT * FROM tabular.dataexpert.anallasw_gold_customer_address_dea

-- COMMAND ----------


