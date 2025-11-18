-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Create company summary in the gold layer
-- MAGIC 1. Create the gold schema if doesn't already exists
-- MAGIC 1. Create the gold company_summary with the number of companies per country. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC CREATE SCHEMA IF NOT EXISTS demo.gold
-- MAGIC      MANAGED LOCATION 'abfss://demo@deacourseextdl.dfs.core.windows.net/gold';  

-- COMMAND ----------

DROP TABLE IF EXISTS tabular.dataexpert.anallasw_gold_companies;
CREATE TABLE tabular.dataexpert.anallasw_gold_companies
AS
SELECT country,
       COUNT(*) AS num_companies
  FROM tabular.dataexpert.anallasw_silver_companies
 GROUP BY country;    
