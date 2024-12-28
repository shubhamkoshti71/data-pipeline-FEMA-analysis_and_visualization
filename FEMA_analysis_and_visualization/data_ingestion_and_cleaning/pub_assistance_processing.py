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
# MAGIC #### Defining schema for processing raw public assistance data fetched from Azure Blob storage
# MAGIC -----

# COMMAND ----------


# Define the schema for the dataset
schema = StructType([
    StructField("disasterNumber", IntegerType(), True),  # smallint -> IntegerType
    StructField("declarationDate", DateType(), True),
    StructField("incidentType", StringType(), True),
    StructField("pwNumber", IntegerType(), True),  # smallint -> IntegerType
    StructField("applicationTitle", StringType(), True),
    StructField("applicantId", StringType(), True),
    StructField("damageCategoryCode", StringType(), True),
    StructField("projectSize", StringType(), True),
    StructField("county", StringType(), True),
    StructField("countyCode", StringType(), True),
    StructField("state", StringType(), True),
    StructField("stateCode", StringType(), True),
    StructField("stateNumberCode", StringType(), True),
    StructField("projectAmount", DecimalType(18, 2), True),  # decimal -> DecimalType(precision, scale)
    StructField("federalShareObligated", DecimalType(18, 2), True),
    StructField("totalObligated", DecimalType(18, 2), True),
    StructField("obligatedDate", DateType(), True),
    StructField("dcc", StringType(), True),
    StructField("damageCategory", StringType(), True),
    StructField("lastRefresh", TimestampType(), True),  # datetimez -> TimestampType
    StructField("hash", StringType(), True),
    StructField("id", StringType(), True)  # uuid -> StringType
])


# COMMAND ----------

# MAGIC %md
# MAGIC ### Processing of raw Public Assistance data
# MAGIC -------------

# COMMAND ----------

pub_assi_stream = (spark.read                                                   #initializing a DataFrameReader to read data using Spark
                    .format("json")                                             #explicitly specifying the format of the data as JSON
                    .schema(schema)                                             #applying schema defined above for data consistency
                    .load(f"{datasets_fema}/raw_files/public_assistance_raw"))  #specifying the location of the raw data

# COMMAND ----------

# Calling the required function defined in the data_cleaning_functions notebook for cleaning the data
pub_assi_cleaned = pub_assi_clean_data(pub_assi_stream)

# COMMAND ----------

# Writing the cleaned data to the desired destination ("/cleaned_files/public_assistance_cleaned")
pub_assi_cleaned.write \
  .format("delta")  \
  .mode("overwrite")  \
  .save(f"{datasets_fema}/cleaned_files/public_assistance_cleaned")

# This piece of code initializes a write operation for the cleaned dataframe and writes it to the desired location
# .format("delta") specifies the format of the data to be written as Delta Lake format, enabling efficient updates
# .mode("overwrite") replaces any existing data in the destination location
# .save() saves the cleaned data to the specified location
