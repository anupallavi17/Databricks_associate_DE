# Databricks notebook source
# MAGIC %md
# MAGIC ### Extract Data From the Memberships - Image Files
# MAGIC - Query Memberships File using binaryFile Format
# MAGIC - Create Memberships View in Bronze Schema

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Query Memberships Folder using binaryFile Format

# COMMAND ----------

# MAGIC %fs
# MAGIC ls '/Volumes/tabular/dataexpert/anallasw/project_gizmobox/landing/operational_data/memberships'
