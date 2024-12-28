# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC ### Executing the spark_config file for accessing the variables in this notebook
# MAGIC -------

# COMMAND ----------

# MAGIC %run ../spark_config

# COMMAND ----------

# MAGIC %md
# MAGIC ### Executing the data cleaning file to access the functions called in this notebook
# MAGIC ---

# COMMAND ----------

# MAGIC %run ./data_cleaning_functions

# COMMAND ----------

# MAGIC %md
# MAGIC #### Defining schema for processing raw hazard mitigation data fetched from Azure Blob storage
# MAGIC -----

# COMMAND ----------

#Creating the schema for the hazard_mitigation dataset
schema = StructType([
    StructField("projectIdentifier", StringType(), True),
    StructField("programArea", StringType(), True),
    StructField("programFy", IntegerType(), True),
    StructField("region", IntegerType(), True),
    StructField("state", StringType(), True),
    StructField("stateNumberCode", StringType(), True),
    StructField("county", StringType(), True),
    StructField("countyCode", StringType(), True),
    StructField("disasterNumber", IntegerType(), True),
    StructField("projectCounties", StringType(), True),
    StructField("projectType", StringType(), True),
    StructField("status", StringType(), True),
    StructField("recipient", StringType(), True),
    StructField("recipientTribalIndicator", BooleanType(), True),
    StructField("subrecipient", StringType(), True),
    StructField("subrecipientTribalIndicator", BooleanType(), True),
    StructField("dataSource", StringType(), True),
    StructField("dateApproved", DateType(), True),
    StructField("dateClosed", DateType(), True),
    StructField("dateInitiallyApproved", DateType(), True),
    StructField("projectAmount", DecimalType(18, 2), True),
    StructField("federalShareObligated", DecimalType(18, 2), True),
    StructField("subrecipientAdminCostAmt", DecimalType(18, 2), True),
    StructField("srmcObligatedAmt", DecimalType(18, 2), True),
    StructField("recipientAdminCostAmt", DecimalType(18, 2), True),
    StructField("costSharePercentage", DecimalType(18, 2), True),
    StructField("benefitCostRatio", DecimalType(18, 2), True),
    StructField("netValueBenefits", DecimalType(18, 2), True),
    StructField("numberOfFinalProperties", IntegerType(), True),
    StructField("numberOfProperties", IntegerType(), True),
    StructField("id", StringType(), True)  # UUID type can be treated as StringType in PySpark
])



# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### Processing of raw Hazard Mitigation data
# MAGIC -------------

# COMMAND ----------

# Reading the hazard_mitigation raw files and storing them in a variable
haz_mit_stream = (spark.read                                                    #initializing a DataFrameReader to read data using Spark
                    .format("json")                                             #explicitly specifying the format of the data as JSON
                    .schema(schema)                                             #applying schema defined above for data consistency
                    .load(f"{datasets_fema}/raw_files/hazard_mitigation_raw"))  #specifying the location of the raw data

# COMMAND ----------

# Calling the required function defined in the data_cleaning_functions notebook for cleaning the data
haz_mit_cleaned = haz_mit_clean_data(haz_mit_stream)
  

# COMMAND ----------

# Writing the cleaned data to the desired destination ("/cleaned_files/hazard_mitigation_cleaned")
haz_mit_cleaned.write \
  .format("delta") \
  .mode("overwrite") \
  .save(f"{datasets_fema}/cleaned_files/hazard_mitigation_cleaned") 

# This piece of code initializes a write operation for the cleaned dataframe and writes it to the desired location
# .format("delta") specifies the format of the data to be written as Delta Lake format, enabling efficient updates
# .mode("overwrite") replaces any existing data in the destination location
# .save() saves the cleaned data to the specified location
