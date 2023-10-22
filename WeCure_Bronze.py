# Databricks notebook source
# MAGIC %md
# MAGIC #This notebook aims to ingest the raw data from ADLS container with the help of a mount point
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Importing all the reqired functions

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

# COMMAND ----------

# MAGIC %md
# MAGIC ##Mounting the data from ADLS

# COMMAND ----------

# container_name = "raw"
# account_name = "wecure"
# storage_acc_key= "IR/Hz2UGlQyW24oyqdHD6xrZ5CC22TLoz7ICRh4hiA4RzpJQ+I3T0UWXxgOJVnqV76GgyhudGh3u+AStdwUOzw=="

# dbutils.fs.mount(
# source = "wasbs://{0}@{1}.blob.core.windows.net".format(container_name, account_name),
# mount_point = "/mnt/wecure",
#   extra_configs = {"fs.azure.account.key.{0}.blob.core.windows.net".format(account_name): storage_acc_key}
#   )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Mounting Streaming data

# COMMAND ----------

# container_name = "hospitalcapstone"
# account_name = "adlsstoragedata01"
# storage_acc_key= "tBwtMqWlyr9ToC74Jxtq1UrA9aFi8fugoJo2SaKHxbwQnSIimMs6QjLW/Xw2Ujpk6M/wb9F9BXeB+AStk6vtGQ=="

# dbutils.fs.mount(
# source = "wasbs://{0}@{1}.blob.core.windows.net".format(container_name, account_name),
# mount_point = "/mnt/wecure/stream",
#   extra_configs = {"fs.azure.account.key.{0}.blob.core.windows.net".format(account_name): storage_acc_key}
#   )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Display content of stream container

# COMMAND ----------

display(dbutils.fs.ls("mnt/wecure/stream"))

# COMMAND ----------

# MAGIC %md
# MAGIC #Ingesting stream data with the help of autoloader

# COMMAND ----------

# This function will create a streaming table for the streaming data which will be received from ADLS
# this is different from normal dlt because, this will be streamed continuosly and will make streaming tables instead of materialized veiw

@dlt.create_table(
  comment="The raw patient device data, ingested from streaming ADLS storage",
  table_properties={
    "wecure_deltaliv.quality": "Bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
# this function will read the data using pyspark and make a DATAFRAME out of it,
# and return the dataframe so that we can create a DLT out of it

def patient_device_stream_raw():
    # Read streaming data from cloudFiles in Parquet format
    # we have use autoloaders for reading the streaming data
    stream_df = spark.readStream.format("cloudFiles") \
                            .option("cloudFiles.format", "parquet") \
                            .option("cloudFiles.schemaLocation", 
                                    "/mnt/wecure/stream/loaders/") \
                            .load("/mnt/wecure/stream/")
    
    # Clean column names by removing leading/trailing spaces and special characters
    new_column_names = [col.strip() for col in stream_df.columns]
    stream_df = stream_df.toDF(*new_column_names)
    new_column_names = [col.lower().replace(" ", "_").replace("(", "_").replace(")", "_").replace(",", "_") for col in stream_df.columns]
    stream_df = stream_df.toDF(*new_column_names)
    new_column_names = [col.replace("__", "_") for col in stream_df.columns]
    stream_df = stream_df.toDF(*new_column_names)
    
    # Rename columns with trailing underscores (if any)
    for old_col_name in stream_df.columns:
        if old_col_name.endswith('_'):
            new_col_name = old_col_name[:-1]  # Remove the trailing '_'
            stream_df = stream_df.withColumnRenamed(old_col_name, new_col_name)
    
    # Return the cleaned DataFrame
    return stream_df


# COMMAND ----------

# MAGIC %md
# MAGIC ##Display all the batch files of the container

# COMMAND ----------

display(dbutils.fs.ls("/mnt/wecure"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Hospitals dataset

# COMMAND ----------

#creating delta live table for the hospital dataset for batch processing

@dlt.create_table(
  comment="The raw hospitals, ingested from ADLS database",
  table_properties={
    "wecure_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def hospitals_raw():
    hospitals_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("dbfs:/mnt/wecure/hospitals.csv")
    return hospitals_df

# COMMAND ----------

# MAGIC %md
# MAGIC ##New_admissions dataset

# COMMAND ----------

# creating DLT for the new_admissions dataset

@dlt.create_table(
  comment="The raw new_admissions, ingested from ADLS database",
  table_properties={
    "wecure_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def new_admissions_raw():
    new_admissions_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("dbfs:/mnt/wecure/new_admissions.csv")

    # We did normalization of the column names everywhere by replacing the special characters with "_" and stripping the names for each column

    new_column_names = [col.lower().replace(" ","_").replace("/","_or_").replace(".","_").replace("-","_") for col in new_admissions_df.columns]
    new_admissions_df = new_admissions_df.toDF(*new_column_names)
    return new_admissions_df

# COMMAND ----------

# MAGIC %md
# MAGIC #Patient history dataset

# COMMAND ----------

# creating patient history DLT

@dlt.create_table(
  comment="The raw patient_history, ingested from ADLS database",
  table_properties={
    "wecure_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def patient_history_raw():
    patient_history_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("dbfs:/mnt/wecure/patient_history.csv")

    # for normalization of the column names, we used lower cases for all the columns
    
    new_column_names = [col.lower().replace(" ","_") for col in patient_history_df.columns]
    patient_history_df = patient_history_df.toDF(*new_column_names)
    return patient_history_df

# COMMAND ----------

# MAGIC %md
# MAGIC #Patients dataset

# COMMAND ----------

# creating delta live table for patients dataset

@dlt.create_table(
  comment="The raw patients, ingested from ADLS database",
  table_properties={
    "wecure_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def patients_raw():
    patients_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("dbfs:/mnt/wecure/patients.csv")
    # normalization of the column names
    
    new_column_names = [col.lower().replace(" ","_") for col in patients_df.columns]
    patients_df = patients_df.toDF(*new_column_names)
    return patients_df

# COMMAND ----------

# MAGIC %md
# MAGIC #Plans dataset

# COMMAND ----------

# creating a delta live table for plans dataset

@dlt.create_table(
  comment="The raw plans, ingested from ADLS database",
  table_properties={
    "wecure_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def plans_raw():
    plans_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("dbfs:/mnt/wecure/plans.csv")
    return plans_df

# COMMAND ----------

# MAGIC %md
# MAGIC #Policies_new dataset
# MAGIC

# COMMAND ----------

#  creating a delta live table for the policies_new dataset

@dlt.create_table(
  comment="The raw policies_new, ingested from ADLS database",
  table_properties={
    "wecure_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def policies_new_raw():
    policies_new_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("dbfs:/mnt/wecure/policies_new.csv")
    return policies_new_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## after creating all the delta live tables of bronze quality, these tables can be used by the silver layers to do cleaning and transaformations and pass it to gold for further analysis

# COMMAND ----------


