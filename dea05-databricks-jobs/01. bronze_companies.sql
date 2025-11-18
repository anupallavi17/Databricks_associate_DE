-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Ingest companies data into bronze layer
-- MAGIC 1. Create the bronze schema if doesn't already exists
-- MAGIC 1. Create the bronze companies table with the data from landing folder. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC CREATE SCHEMA IF NOT EXISTS demo.bronze
-- MAGIC      MANAGED LOCATION 'abfss://demo@deacourseextdl.dfs.core.windows.net/bronze';  

-- COMMAND ----------

DROP TABLE IF EXISTS tabular.dataexpert.anallasw_bronze_companies;

CREATE TABLE tabular.dataexpert.anallasw_bronze_companies
AS
SELECT company_name, founded_date, country
  FROM read_files('/Volumes/tabular/dataexpert/anallasw/demo/landing/companies/top_tech_companies_1.csv',
                  format => 'csv',
                  header => true);
