# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tabular.dataexpert.delta_lake_companies_anallasw

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE tabular.dataexpert.delta_lake_companies_anallasw
# MAGIC  (company_name STRING,
# MAGIC    founded_date DATE,
# MAGIC    country      STRING)
# MAGIC COMMENT 'This table contains information about some of the successful tech companies'   
# MAGIC TBLPROPERTIES ('sensitive' = 'true', 'delta.enableDeletionVectors' = 'false');

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Column Properties
# MAGIC -   2.1 NOT NULL Constraints - enforces data integrity and quality by ensuring that a specific column cannot contain NULL values
# MAGIC -   2.2 COMMENT - documents the purpose or context of individual columns in a table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tabular.dataexpert.delta_lake_companies_anallasw;
# MAGIC CREATE TABLE tabular.dataexpert.delta_lake_companies_anallasw
# MAGIC  (company_name STRING NOT NULL,
# MAGIC    founded_date DATE COMMENT 'The date the company was founded',
# MAGIC    country      STRING)
# MAGIC COMMENT 'This table contains information about some of the successful tech companies'   
# MAGIC TBLPROPERTIES ('sensitive' = 'true', 'delta.enableDeletionVectors' = 'false');

# COMMAND ----------

# MAGIC %sql DESC EXTENDED tabular.dataexpert.delta_lake_companies_anallasw;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Generated Columns - derived or computed columns, whose values are computed at the time of inserting a new records
# MAGIC - 2.3.1. Generated Identity Columns - used to generate an identity for example a primary key value
# MAGIC - 2.3.2. Generated Computed Columns - automatically calculate and store derived values based on other columns in the same table.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generated Identity Columns
# MAGIC GENERATED { ALWAYS | BY DEFAULT } AS IDENTITY [ ( [ START WITH start ] [ INCREMENT BY step ] ) ]

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tabular.dataexpert.delta_lake_companies_anallasw;
# MAGIC CREATE TABLE tabular.dataexpert.delta_lake_companies_anallasw
# MAGIC  (company_id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1 ),
# MAGIC   company_name STRING NOT NULL,
# MAGIC    founded_date DATE COMMENT 'The date the company was founded',
# MAGIC    country      STRING)
# MAGIC COMMENT 'This table contains information about some of the successful tech companies'   
# MAGIC TBLPROPERTIES ('sensitive' = 'true', 'delta.enableDeletionVectors' = 'false');

# COMMAND ----------

# MAGIC %sql 
# MAGIC Insert into tabular.dataexpert.delta_lake_companies_anallasw
# MAGIC (company_name, founded_date, country)
# MAGIC VALUES ("Apple", "1976-04-01", "USA"),
# MAGIC        ("Microsoft", "1975-04-04", "USA"),
# MAGIC        ("Google", "1998-09-04", "USA"),
# MAGIC        ("Amazon", "1994-07-05", "USA");

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from tabular.dataexpert.delta_lake_companies_anallasw;

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.3.2. Generated Computed Columns
# MAGIC GENERATED ALWAYS AS ( `expr` )
# MAGIC
# MAGIC `expr` may be composed of literals, column identifiers within the table, and deterministic, built-in SQL functions or operators except:
# MAGIC - Aggregate functions
# MAGIC - Analytic window functions
# MAGIC - Ranking window functions
# MAGIC - Table valued generator functions
# MAGIC
# MAGIC Also `expr` must not contain any subquery.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tabular.dataexpert.delta_lake_companies_anallasw;
# MAGIC CREATE TABLE tabular.dataexpert.delta_lake_companies_anallasw
# MAGIC  (company_id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1 ),
# MAGIC   company_name STRING NOT NULL,
# MAGIC    founded_date DATE COMMENT 'The date the company was founded',
# MAGIC    founded_year INT GENERATED ALWAYS AS (YEAR(founded_date)) COMMENT 'The year the company was founded',
# MAGIC    country      STRING)
# MAGIC COMMENT 'This table contains information about some of the successful tech companies'   
# MAGIC TBLPROPERTIES ('sensitive' = 'true', 'delta.enableDeletionVectors' = 'false');

# COMMAND ----------

# MAGIC %sql 
# MAGIC Insert into tabular.dataexpert.delta_lake_companies_anallasw
# MAGIC (company_name, founded_date, country)
# MAGIC VALUES ("Apple", "1976-04-01", "USA"),
# MAGIC        ("Microsoft", "1975-04-04", "USA"),
# MAGIC        ("Google", "1998-09-04", "USA"),
# MAGIC        ("Amazon", "1994-07-05", "USA");

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tabular.dataexpert.delta_lake_companies_anallasw;

# COMMAND ----------


