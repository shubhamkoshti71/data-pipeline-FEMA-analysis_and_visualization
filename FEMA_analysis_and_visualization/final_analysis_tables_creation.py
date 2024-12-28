# Databricks notebook source
# MAGIC %run ./spark_config

# COMMAND ----------

# MAGIC %md
# MAGIC ### DDLs of tables to be created for final analysis
# MAGIC ---------------

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DDL for hazard mitigation analysis table
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS hazmit_analysis (
# MAGIC     id STRING,
# MAGIC     projectIdentifier STRING,
# MAGIC     programArea STRING,
# MAGIC     programFy INT,
# MAGIC     region INT,
# MAGIC     state STRING,
# MAGIC     stateNumberCode STRING,
# MAGIC     projectType STRING,
# MAGIC     status STRING,
# MAGIC     dataSource STRING,
# MAGIC     dateApproved DATE,
# MAGIC     dateClosed DATE,
# MAGIC     projectAmount DECIMAL(18,2),
# MAGIC     costSharePercentage DECIMAL(18,2),
# MAGIC     benefitCostRatio DECIMAL(18,2),
# MAGIC     netValueBenefits DECIMAL(18,2),
# MAGIC     numberOfFinalProperties INT,
# MAGIC     numberOfProperties INT
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DDL for public assistance analysis table
# MAGIC CREATE TABLE IF NOT EXISTS pub_assistance_analysis (
# MAGIC   id STRING,
# MAGIC   disasterNumber INT,
# MAGIC   declarationDate DATE,
# MAGIC   incidentType STRING,
# MAGIC   damageCategory STRING,
# MAGIC   projectSize STRING,
# MAGIC   county STRING,
# MAGIC   state STRING,
# MAGIC   stateCode STRING,
# MAGIC   projectAmount DECIMAL(18,2),
# MAGIC   federalShareObligated DECIMAL(18,2),
# MAGIC   totalObligated DECIMAL(18,2),
# MAGIC   obligatedDate DATE
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DDL for disaster declaration analysis table
# MAGIC CREATE TABLE IF NOT EXISTS disaster_declaration_analysis (
# MAGIC   id STRING,
# MAGIC   disasterNumber INT,
# MAGIC   fipsStateCode STRING,
# MAGIC   state STRING,
# MAGIC   fipsCountyCode STRING,
# MAGIC   designatedArea STRING,
# MAGIC   declarationRequestNumber STRING,
# MAGIC   declarationType STRING,
# MAGIC   declarationTitle STRING,
# MAGIC   declarationDate DATE,
# MAGIC   fyDeclared INT,
# MAGIC   incidentId STRING,
# MAGIC   incidentType STRING,
# MAGIC   incidentBeginDate DATE,
# MAGIC   incidentEndDate DATE,
# MAGIC   tribalRequest STRING,
# MAGIC   ihProgramDeclared STRING,
# MAGIC   iaProgramDeclared STRING,
# MAGIC   paProgramDeclared STRING,
# MAGIC   hmProgramDeclared STRING
# MAGIC );
# MAGIC   
# MAGIC   
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Temporary views creation for fetching contents from cleaned files folder
# MAGIC ----------------

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC -- temporary view for storing hazard mitigation cleaned data files
# MAGIC create or replace temp view temp_haz_mit_cleaned AS
# MAGIC select id, 
# MAGIC   projectIdentifier, 
# MAGIC   programArea, 
# MAGIC   programFy,
# MAGIC   region,
# MAGIC   state,
# MAGIC   stateNumberCode,
# MAGIC   projectType,
# MAGIC   status,
# MAGIC   dataSource,
# MAGIC   dateApproved,
# MAGIC   dateClosed,
# MAGIC   projectAmount,
# MAGIC   costSharePercentage,
# MAGIC   benefitCostRatio,
# MAGIC   netValueBenefits,
# MAGIC   numberOfFinalProperties,
# MAGIC   numberOfProperties
# MAGIC  from delta.`${fema.datasets}/cleaned_files/hazard_mitigation_cleaned`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- temporary view for storing public assistance cleaned data files
# MAGIC CREATE OR REPLACE TEMP VIEW temp_pub_assistance_cleaned AS
# MAGIC SELECT
# MAGIC   id,
# MAGIC   disasterNumber,
# MAGIC   declarationDate,
# MAGIC   incidentType,
# MAGIC   damageCategory,
# MAGIC   projectSize,
# MAGIC   county,
# MAGIC   state,
# MAGIC   stateCode,
# MAGIC   projectAmount,
# MAGIC   federalShareObligated,
# MAGIC   totalObligated,
# MAGIC   obligatedDate
# MAGIC FROM delta.`${fema.datasets}/cleaned_files/public_assistance_cleaned`

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC -- temporary view for storing disaster declaration cleaned data files
# MAGIC CREATE OR REPLACE TEMP VIEW temp_dis_decl_cleaned AS
# MAGIC SELECT id,
# MAGIC        disasterNumber,
# MAGIC        fipsStateCode,
# MAGIC        state,
# MAGIC        fipsCountyCode,
# MAGIC        designatedArea,
# MAGIC        declarationRequestNumber,
# MAGIC        declarationType,
# MAGIC        declarationTitle,
# MAGIC        declarationDate,
# MAGIC        fyDeclared,
# MAGIC        incidentId,
# MAGIC        incidentType,
# MAGIC        incidentBeginDate,
# MAGIC        incidentEndDate,
# MAGIC        tribalRequest,
# MAGIC        ihProgramDeclared,
# MAGIC        iaProgramDeclared,
# MAGIC        paProgramDeclared,
# MAGIC        hmProgramDeclared
# MAGIC from DELTA.`${fema.datasets}/cleaned_files/dis_decl_cleaned`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Updating the tables for final analysis
# MAGIC ---------------
# MAGIC <b> INSERT OVERWRITE: </b> This is used to achieve accuracy and consistency in the target table by updating it by overwriting the existing data with the new/updated incoming data. \
# MAGIC <b> Advantages: </b>
# MAGIC 1. Ensures that the table contains up-to-date information
# MAGIC 2. Prevents duplication and ensures that the analysis is based on the most recent information
# MAGIC 3. Allows for incremental updates to the existing dataset
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT OVERWRITE disaster_declaration_analysis
# MAGIC SELECT * FROM temp_dis_decl_cleaned

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT OVERWRITE pub_assistance_analysis
# MAGIC SELECT * FROM temp_pub_assistance_cleaned

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE hazmit_analysis
# MAGIC SELECT * FROM temp_haz_mit_cleaned
