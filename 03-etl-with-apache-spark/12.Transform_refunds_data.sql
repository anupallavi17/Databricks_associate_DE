-- Databricks notebook source
select * from tabular.dataexpert.anallasw_bronze_refunds_dea

-- COMMAND ----------

select refund_id,
payment_id,
refund_timestamp,
refund_amount,
split(refund_reason,':')[0] as refund_reason,
split(refund_reason,':')[1] as refund_source
from tabular.dataexpert.anallasw_bronze_refunds_dea


-- COMMAND ----------

select refund_id,
payment_id,
refund_timestamp,
refund_amount,
regexp_extract(refund_reason, '^([^:]+):', 1) AS refund_reason,
regexp_extract(refund_reason, '^([^:]+):(.*)$', 1) AS refund_reason
from tabular.dataexpert.anallasw_bronze_refunds_dea

-- COMMAND ----------

select refund_id,
payment_id,
CAST(date_format(refund_timestamp,'yyyy-MM-dd') as date) as refund_date,
date_format(refund_timestamp,'HH:mm:ss') as refund_time,
refund_amount,
regexp_extract(refund_reason, '^([^:]+):', 1) AS refund_reason,
regexp_extract(refund_reason, '^[^:]+:(.*)$', 1) AS refund_source
from tabular.dataexpert.anallasw_bronze_refunds_dea

-- COMMAND ----------

create table if not exists tabular.dataexpert.anallasw_silver_refunds_dea
as
select refund_id,
payment_id,
CAST(date_format(refund_timestamp,'yyyy-MM-dd') as date) as refund_date,
date_format(refund_timestamp,'HH:mm:ss') as refund_time,
refund_amount,
regexp_extract(refund_reason, '^([^:]+):', 1) AS refund_reason,
regexp_extract(refund_reason, '^[^:]+:(.*)$', 1) AS refund_source
from tabular.dataexpert.anallasw_bronze_refunds_dea

-- COMMAND ----------

select * from tabular.dataexpert.anallasw_silver_refunds_dea

-- COMMAND ----------


