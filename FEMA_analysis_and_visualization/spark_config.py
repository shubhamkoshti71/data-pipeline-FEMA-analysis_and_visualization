# Databricks notebook source
# MAGIC %md
# MAGIC ### Setting config variables for global access to the datasets

# COMMAND ----------

source_path = '/mnt/fema_files_raw'
datasets_fema = 'dbfs:/mnt/fema_datasets'
catalog_name = 'hive_metastore'
spark.conf.set(f'fema.datasets', datasets_fema)
