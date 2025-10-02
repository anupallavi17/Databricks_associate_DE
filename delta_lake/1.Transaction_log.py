# Databricks notebook source
# MAGIC %sql
# MAGIC create table if not exists tabular.dataexpert.delta_lake_companies_anallasw
# MAGIC (company_name string,
# MAGIC  founded_date Date,
# MAGIC  country string)

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended tabular.dataexpert.delta_lake_companies_anallasw

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT into tabular.dataexpert.delta_lake_companies_anallasw
# MAGIC values('Microsoft', '1975-04-04', 'USA');

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into tabular.dataexpert.delta_lake_companies_anallasw
# MAGIC values('Google', '1998-09-04', 'USA'),
# MAGIC        ('Amazon', '1994-05-05', 'USA');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tabular.dataexpert.delta_lake_companies_anallasw

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history tabular.dataexpert.delta_lake_companies_anallasw;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tabular.dataexpert.delta_lake_companies_anallasw
# MAGIC version AS OF 1;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from tabular.dataexpert.delta_lake_companies_anallasw
# MAGIC timestamp as of '2025-09-10T17:30:26.000+00:00'

# COMMAND ----------

# MAGIC %sql 
# MAGIC restore table tabular.dataexpert.delta_lake_companies_anallasw 
# MAGIC version as of 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tabular.dataexpert.delta_lake_companies_anallasw

# COMMAND ----------


