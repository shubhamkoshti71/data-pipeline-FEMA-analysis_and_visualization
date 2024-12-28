# Databricks notebook source
# MAGIC %md
# MAGIC #### Code to drop tables in case of bad data inserstions
# MAGIC ---

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use hive_metastore.fema_database;
# MAGIC -- drop table if exists pub_assistance_analysis;
# MAGIC
# MAGIC -- drop table if exists hazmit_analysis;
# MAGIC
# MAGIC -- drop table if exists disaster_declaration_analysis;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ensuring whether data has been populated in the target tables or not
# MAGIC ----

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hazmit_analysis
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from pub_assistance_analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from disaster_declaration_analysis

# COMMAND ----------

# MAGIC %md
# MAGIC #### Filterning the tables to check for duplicates
# MAGIC -----

# COMMAND ----------

# MAGIC %sql
# MAGIC select id, count(id) from pub_assistance_analysis
# MAGIC group by id having count(id) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select id, count(id) from hazmit_analysis
# MAGIC group by id having count(id) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select id, count(id) from disaster_declaration_analysis
# MAGIC group by id having count(id) > 1
