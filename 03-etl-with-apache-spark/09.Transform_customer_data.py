# Databricks notebook source
# MAGIC %md
# MAGIC ### 1.Remove records with NULL customer_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tabular.dataexpert.anallasw_bronze_customer_dea where customer_id is NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Remove exact duplicate records

# COMMAND ----------

# MAGIC %sql
# MAGIC select DISTINCT * from tabular.dataexpert.anallasw_bronze_customer_dea where customer_id is NOT NULL order by customer_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATe or REPLACE TEMPORARY VIEW anallasw_bronze_customer_dea_distinct_v as select DISTINCT * from tabular.dataexpert.anallasw_bronze_customer_dea where customer_id is not null order by customer_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte_max_timestamp as (
# MAGIC select customer_id, max(created_timestamp) as max_created_timestamp from anallasw_bronze_customer_dea_distinct_v GROUP BY customer_id)
# MAGIC select v.customer_id, v.created_timestamp, v.customer_name, v.date_of_birth, v.email, v.member_since, v.telephone
# MAGIC FROM 
# MAGIC cte_max_timestamp  c 
# MAGIC join anallasw_bronze_customer_dea_distinct_v  v 
# MAGIC on c.customer_id = v.customer_id 
# MAGIC AND c.max_created_timestamp = v.created_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ### CAST the column value to correct datatype

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte_max_timestamp as (
# MAGIC select customer_id, max(created_timestamp) as max_created_timestamp from anallasw_bronze_customer_dea_distinct_v GROUP BY customer_id)
# MAGIC select 
# MAGIC v.customer_id, 
# MAGIC CAST(v.created_timestamp AS timestamp) as created_timestamp,
# MAGIC v.customer_name, 
# MAGIC CAST(v.date_of_birth AS date) as date_of_birth, v.email, 
# MAGIC CAST(v.member_since AS date) as member_since, 
# MAGIC v.telephone
# MAGIC FROM 
# MAGIC cte_max_timestamp  c 
# MAGIC join anallasw_bronze_customer_dea_distinct_v  v 
# MAGIC on c.customer_id = v.customer_id 
# MAGIC AND c.max_created_timestamp = v.created_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write data to Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS tabular.dataexpert.anallasw_silver_customer_dea 
# MAGIC AS 
# MAGIC with cte_max_timestamp as (
# MAGIC select customer_id, max(created_timestamp) as max_created_timestamp from anallasw_bronze_customer_dea_distinct_v GROUP BY customer_id)
# MAGIC select 
# MAGIC v.customer_id, 
# MAGIC CAST(v.created_timestamp AS timestamp) as created_timestamp,
# MAGIC v.customer_name, 
# MAGIC CAST(v.date_of_birth AS date) as date_of_birth, v.email, 
# MAGIC CAST(v.member_since AS date) as member_since, 
# MAGIC v.telephone
# MAGIC FROM 
# MAGIC cte_max_timestamp  c 
# MAGIC join anallasw_bronze_customer_dea_distinct_v  v 
# MAGIC on c.customer_id = v.customer_id 
# MAGIC AND c.max_created_timestamp = v.created_timestamp

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tabular.dataexpert.anallasw_silver_customer_dea;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED tabular.dataexpert.anallasw_silver_customer_dea;

# COMMAND ----------


