# Databricks notebook source
# MAGIC %md
# MAGIC ### Defining the required libraries for processing
# MAGIC ------------

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, TimestampType, BooleanType

# COMMAND ----------

# MAGIC %md
# MAGIC ### Functions for cleaning the data coming from: 

# COMMAND ----------

# MAGIC %md
# MAGIC #### hazard_mitigation stream
# MAGIC ----------

# COMMAND ----------

def haz_mit_clean_data(df):
  #excluding the columns that are not needed for further analysis
  exclude_cols = ['county', 'countyCode', 'recipient', 'projectCounties', 'disasterNumber', 'recipientTribalIndicator', 'subrecipient', 'subrecipientTribalIndicator', 'dateInitiallyApproved', 'initialObligationDate', 'initialObligationAmount', 'federalShareObligated', 'subrecipientAdminCostAmt', 'srmcObligatedAmt', 'recipientAdminCostAmt']
  hazard_mit_cleaned = df.select([col for col in df.columns if col not in exclude_cols])

  #removing rows with missing values for specific columns
  hazard_mit_cleaned = hazard_mit_cleaned.dropna(subset=['costSharePercentage', 'benefitCostRatio', 'netValueBenefits', 'projectAmount', 'status'])

  #replacing rest of the missing values with "Unknown"
  hazard_mit_cleaned = hazard_mit_cleaned.fillna("Unknown")

  return hazard_mit_cleaned
  

# COMMAND ----------

# MAGIC %md
# MAGIC #### disaster_declaration stream
# MAGIC --------------

# COMMAND ----------

# importing functions and assigning an alias for better readability
from pyspark.sql import functions as F

def dis_decl_clean_data(df):
  #excluding the columns that are not needed for further analysis
  exclude_cols = ['femaDeclarationString', 'disasterCloseoutDate', 'lastIAFilingDate', 'designatedIncidentTypes', 'lastRefresh', 'hash', 'placeCode']
  disaster_dec_cleaned = df.select([col for col in df.columns if col not in exclude_cols])

  #removing rows with missing values for the column incidentEndDate, since cannot exclude this column completely
  disaster_dec_cleaned = disaster_dec_cleaned.dropna(subset=['incidentEndDate'])

  #replacing boolean values in few columns to Yes/No for better readability and ease of visualizations
  for col in ['ihProgramDeclared', 'iaProgramDeclared', 'paProgramDeclared', 'hmProgramDeclared','tribalRequest']:
    disaster_dec_cleaned = disaster_dec_cleaned.withColumn(col, F.when(F.col(col) == True, 'Yes').otherwise('No'))
  
  return disaster_dec_cleaned

# COMMAND ----------

# MAGIC %md
# MAGIC #### public_assistance stream
# MAGIC ------------

# COMMAND ----------

def pub_assi_clean_data(df):
  #excluding the columns that are not needed for further analysis
  exclude_cols = ['hash', 'lastRefresh', 'pwNumber', 'applicationTitle', 'dcc']
  public_assi_cleaned = df.select([col for col in df.columns if col not in exclude_cols])

  #removing rows with missing values for columns 'county' and 'countyCode', as these columns can be useful in further analysis
  public_assi_cleaned = public_assi_cleaned.dropna(subset=['county', 'countyCode'])

  return public_assi_cleaned


