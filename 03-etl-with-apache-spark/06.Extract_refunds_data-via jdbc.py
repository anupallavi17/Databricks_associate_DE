# Databricks notebook source
# MAGIC %md
# MAGIC CREATE SCHEMA IF NOT EXISTS hive_metastore.bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC CREATE TABLE IF NOT EXISTS hive_metastore.bronze.refunds
# MAGIC USING jdbc
# MAGIC OPTIONS(URL "",-------------------> get from azure database connection string(until db name)
# MAGIC dbtable 'refunds',
# MAGIC user 'gizmoboxadm'
# MAGIC password 'Gizmoboxadm')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE tabular.dataexpert.anallasw_bronze_refunds_dea (
# MAGIC   refund_id INT,
# MAGIC   payment_id INT,
# MAGIC   refund_timestamp TIMESTAMP,
# MAGIC   refund_amount DOUBLE,
# MAGIC   refund_reason STRING
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO tabular.dataexpert.anallasw_bronze_refunds_dea VALUES
# MAGIC (1, 66, '2025-01-10T11:30:00.000+00:00', 85.75, 'Payment Error:Retailer'),
# MAGIC (2, 69, '2025-01-03T12:40:15.000+00:00', 120.50, 'Order Cancelled:Customer'),
# MAGIC (3, 72, '2025-01-06T14:45:30.000+00:00', 65.00, 'Product Returned:Customer'),
# MAGIC (4, 73, '2025-01-07T16:10:45.000+00:00', 210.99, 'Order Cancelled:Customer'),
# MAGIC (5, 75, '2025-01-09T18:25:00.000+00:00', 45.20, 'Payment Error:Retailer'),
# MAGIC (6, 80, '2025-01-10T09:35:20.000+00:00', 130.15, 'Order Cancelled:Customer'),
# MAGIC (7, 83, '2025-01-12T11:20:40.000+00:00', 150.00, 'Product Returned:Customer'),
# MAGIC (8, 85, '2025-01-14T13:15:30.000+00:00', 89.99, 'Order Cancelled:Customer'),
# MAGIC (9, 89, '2025-01-15T15:00:00.000+00:00', 78.50, 'Payment Error:Retailer'),
# MAGIC (10, 91, '2025-01-17T16:45:15.000+00:00', 250.75, 'Product Returned:Customer');

# COMMAND ----------


