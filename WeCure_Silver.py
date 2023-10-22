# Databricks notebook source
# MAGIC %md
# MAGIC #This notebook aims to take the bronze DLTs as inputs and do cleaning operations and check data quality issues. We can set the expectations in this step itself

# COMMAND ----------

# MAGIC %md
# MAGIC ##Importing all the necessary functions

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt
import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ##The below function takes DF and the date column which is not in dd-MM-yyyy format and outputs it in a proper format in all DLTs

# COMMAND ----------

def modify_date_column(df, col_name):
    # Split the date string into day, month, and year components
    day_component = split(col(col_name), "/")[1]
    month_component = split(col(col_name), "/")[0]
    year_component = split(col(col_name), "/")[2]

    # Check if day or month is less than 10, and add leading "0" if necessary
    day_component = when(day_component.cast("int") < 10, concat(lit("0"), day_component)).otherwise(day_component)
    month_component = when(month_component.cast("int") < 10, concat(lit("0"), month_component)).otherwise(month_component)

    # Create a new column with the modified date format
    df = df.withColumn(col_name,
        when(col(col_name).contains("/"),
            concat(day_component, lit("-"), month_component, lit("-"), year_component)
        )
        .otherwise(col(col_name))
    )
    return df




# COMMAND ----------

# MAGIC %md
# MAGIC #Hospital

# COMMAND ----------

#Creating the delta live table of silver quality by reading the bronze quality table in wecure schema
@dlt.create_table(
  comment="The cleaned hospitals, ingested from delta",
 
  table_properties={
    "wecure_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
#setting expectation on hosp_id to be not null
@dlt.expect_or_drop("valid hosp_id", "hosp_id IS NOT NULL")
def hospitals_clean():
    hospitals_df=dlt.read("hospitals_raw")

    #querry to make pedi_icu_beds=0 
    hospitals_df = hospitals_df.withColumn("pedi_icu_beds",when((col("adult_icu_beds") + col("pedi_icu_beds")) != col("num_icu_beds"), 0).otherwise(col("pedi_icu_beds")))

    #querry to make num staffed bed = num licensed bed 
    hospitals_df = hospitals_df.withColumn("num_staffed_beds",when(col("num_staffed_beds") > col("num_licensed_beds"), col("num_licensed_beds")).otherwise(col("num_staffed_beds")))
    
    return hospitals_df

# COMMAND ----------

# MAGIC %md
# MAGIC #new_admissions

# COMMAND ----------

# creating the new_admissions DLT of silver quality

@dlt.create_table(
  comment="The cleaned new_admissions, ingested from delta",
  table_properties={
    "wecure_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
def new_admissions_clean():
    new_admissions_df=dlt.read("new_admissions_raw")

    #harmonizing the date format
    new_admissions_df = modify_date_column(new_admissions_df, "d_o_a")
    
    return new_admissions_df

# COMMAND ----------

# MAGIC %md
# MAGIC #patient_history

# COMMAND ----------

# creating the patient_history cleaned DLT of silver quality

@dlt.create_table(
  comment="The cleaned patient_history, ingested from delta",
  table_properties={
    "wecure_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
def patient_history_clean():
    # this data didn't require any changes in column names , nor had data quality issues
    patient_history_df=dlt.read("patient_history_raw")

    return patient_history_df

# COMMAND ----------

# MAGIC %md
# MAGIC #patients

# COMMAND ----------

# creating the patient cleaned DLT of silver quality

@dlt.create_table(
  comment="The cleaned patients, ingested from delta",
  table_properties={
    "wecure_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
def patients_df_clean():

    patients_df=dlt.read("patients_raw")

    #harmonizing the date format
    patients_df = modify_date_column(patients_df, "dob")

    #dropping some duplicates if any
    patients_df=patients_df.dropDuplicates()

    #fixing data consistency issue which had an occurance as below
    patients_df=patients_df.filter((col("full_name") != "Prerak Kamdar") & (col("patient_id") != "XDZ03BA4Y3"))
    return patients_df

# COMMAND ----------

# MAGIC %md
# MAGIC #Plans

# COMMAND ----------

# creating the plans cleaned DLT of silver quality

@dlt.create_table(
  comment="The cleaned plans, ingested from delta",
  table_properties={
    "wecure_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
def plans_clean():
    plans_df=dlt.read("plans_raw")
    return plans_df

# COMMAND ----------

# MAGIC %md
# MAGIC #Policies_new

# COMMAND ----------

# creating the policies cleaned DLT of silver quality

@dlt.create_table(
  comment="The cleaned policies_new, ingested from delta",
  table_properties={
    "wecure_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
def policies_new_clean():
    policies_new_df=dlt.read("policies_new_raw")
    return policies_new_df

# COMMAND ----------

# MAGIC %md
# MAGIC #streaming patient device data

# COMMAND ----------

# creating a cleaned streaming patient device DLT from the streaming raw DLT

@dlt.create_table(
  comment="The clean patient device data, ingested from streaming adsl storage",
 
  table_properties={
    "wecure_deltaliv.quality": "Silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
#setting expectation to warn when heart beat goes too high
@dlt.expect_or_warn("heart_rate_valid", "col(heart_rate_per_minute) < '190'")

def patient_device_stream_clean():
    stream_df = dlt.read_stream("patient_device_stream_raw")
    return stream_df


# COMMAND ----------

# MAGIC %md
# MAGIC ## after cleaning the data in silver form and checking all the data quality issue, this data can be consumed for creating facts table of gold quality and do some business intelligence queries on top of it

# COMMAND ----------


