# Databricks notebook source
# MAGIC %md
# MAGIC ### Executing the spark_config file for accessing the variables in this notebook
# MAGIC -------

# COMMAND ----------

# MAGIC %run ../spark_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Executing the data cleaning file to access the functions called in this notebook
# MAGIC ---

# COMMAND ----------

# MAGIC %run ./data_cleaning_functions

# COMMAND ----------

# MAGIC %md
# MAGIC #### Defining schema for processing raw disaster declarations data fetched from Azure Blob storage
# MAGIC -----

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType, DateType

#Creating the schema for the disaster_declarations dataset

schema = StructType([
  StructField("femaDeclarationString", StringType(), True),
  StructField("disasterNumber", IntegerType(), True),
  StructField("state", StringType(), True),
  StructField("declarationType", StringType(), True),
  StructField("declarationDate", DateType(), True),
  StructField("fyDeclared", IntegerType(), True),
  StructField("incidentType", StringType(), True),
  StructField("declarationTitle", StringType(), True),
  StructField("ihProgramDeclared", BooleanType(), True),
  StructField("iaProgramDeclared", BooleanType(), True),
  StructField("paProgramDeclared", BooleanType(), True),
  StructField("hmProgramDeclared", BooleanType(), True),
  StructField("incidentBeginDate", DateType(), True),
  StructField("incidentEndDate", DateType(), True),
  StructField("disasterCloseoutDate", DateType(), True),
  StructField("tribalRequest", BooleanType(), True),
  StructField("fipsStateCode", StringType(), True),
  StructField("fipsCountyCode", StringType(), True),
  StructField("placeCode", StringType(), True),
  StructField("designatedArea", StringType(), True),
  StructField("declarationRequestNumber", StringType(), True),
  StructField("lastIAFilingDate", DateType(), True),
  StructField("incidentId", StringType(), True),
  StructField("region", IntegerType(), True),
  StructField("designatedIncidentTypes", StringType(), True),
  StructField("lastRefresh", TimestampType(), True),
  StructField("hash", StringType(), True),
  StructField("id", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Processing of raw Disaster Declarations data
# MAGIC -------------

# COMMAND ----------

# Reading the disaster_declarations raw files and storing them in a variable
dis_decl_stream = (spark.read                                                     #initializing a DataFrameReader to read data using Spark
                        .format("json")                                           #explicitly specifying the format of the data as JSON
                        .schema(schema)                                           #applying schema defined above for data consistency
                        .load(f"{datasets_fema}/raw_files/disaster_decl_raw"))    #specifying the location of the raw data

# COMMAND ----------

# Calling the required function defined in the data_cleaning_functions notebook for cleaning the data
dis_decl_cleaned = dis_decl_clean_data(dis_decl_stream)


# COMMAND ----------

# Writing the cleaned data to the desired destination ("/cleaned_files/dis_decl_cleaned")
dis_decl_cleaned.write \                                                        
  .format("delta") \
  .mode("overwrite") \
  .save(f"{datasets_fema}/cleaned_files/dis_decl_cleaned")

# This piece of code initializes a write operation for the cleaned dataframe and writes it to the desired location
# .format("delta") specifies the format of the data to be written as Delta Lake format, enabling efficient updates
# .mode("overwrite") replaces any existing data in the destination location
# .save() saves the cleaned data to the specified location
