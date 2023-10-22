# Databricks notebook source
# MAGIC %md
# MAGIC #This is the final notebook for the ETL pipeline which takes all the cleaned data and create facts table out of it. This aggregated facts can be used for BI applications

# COMMAND ----------

# MAGIC %md
# MAGIC ##Importing all the required functions

# COMMAND ----------


from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt


# COMMAND ----------

# MAGIC %md
# MAGIC ##creating the hospitals aggregated facts table of GOLD quality

# COMMAND ----------

@dlt.create_table(
  comment="The hospitals aggregated facts",
  table_properties={
    "wecure_deltaliv.quality": "gold",
    "pipelines.autoOptimize.managed": "true"
  }
)

def hospital_agg_facts():
    #read all the clean table
    new_admissions_clean=dlt.read("new_admissions_clean")
    patients_df_clean=dlt.read("patients_df_clean")
    patient_history_clean=dlt.read("patient_history_clean")
    hospitals_clean=dlt.read("hospitals_clean")

    #Joining all the tables
    hospitals_agg_df=new_admissions_clean.join(patients_df_clean, on="patient_id",how="inner")
    hospitals_agg_df=hospitals_agg_df.join(hospitals_clean,hospitals_clean.hosp_id==new_admissions_clean.hospital_id,how="inner")
    hospitals_agg_df=hospitals_agg_df.join(patient_history_clean,on="patient_id",how="inner")

    # Add a new "icu_admission" column based on the duration_of_intensive_unit_stay
    hospitals_agg_df=hospitals_agg_df.withColumn("icu_admission", when(col("duration_of_intensive_unit_stay")>0,1).otherwise(0))

    # Add a new "mortality_rate" column based on the outcome
    hospitals_agg_df=hospitals_agg_df.withColumn("mortality_rate", when(col("outcome")=="EXPIRY",1).otherwise(0))

    heart_disease_conditions = (
        (col("cad") > 0) |
        (col("prior_cmp") > 0) |
        (col("heart_failure") > 0) |
        (col("hfref") > 0) |
        (col("hfnef") > 0) |
        (col("valvular") > 0) |
        (col("chb") > 0) |
        (col("sss") > 0) |
        (col("af") > 0)
    )

    # Add a new "heart_disease" column based on the conditions
    hospitals_agg_df = hospitals_agg_df.withColumn("heart_disease", when(heart_disease_conditions, 1).otherwise(0))

    lung_disease_conditions = (
        (col("smoking_") > 0) |
        (col("alcohol") > 0) |
        (col("chest_infection") > 0) |
        (col("pulmonary_embolism") > 0) 
    )

    # Add a new "lung_disease" column based on the conditions
    hospitals_agg_df = hospitals_agg_df.withColumn("lung_disease", when(lung_disease_conditions, 1).otherwise(0))

    # Group by "hospital_id" and perform aggregations
    hospitals_agg_df = hospitals_agg_df.groupBy("hospital_id").agg(
    count("admission_id").alias("total_admission"),  # Count total admissions

    sum(col("icu_admission")).alias("total_icu_admission"),  # Sum ICU admissions

    round(sum("mortality_rate") / count("admission_id"), 2).alias("mortality_rate"),  # Calculate mortality rate

    round(avg("duration_of_intensive_unit_stay")).alias("avg_day_in_icu"),  # Calculate average days in ICU

    sum("surgery_theatres").alias("no_of_surgical_theatre"),  # Sum surgical theaters

    round(avg("duration_of_stay")).alias("avg_day_of_stay"),  # Calculate average days of stay

    sum("ventilators_available").alias("no_of_vantilators"),  # Sum available ventilators

    round(sum("num_staffed_beds") / sum("num_licensed_beds"), 2).alias("staffed_bed_to_normal_bed_ratio"),  # Calculate bed ratio

    round(sum("num_icu_beds") / sum("num_licensed_beds"), 2).alias("icu_bed_to_normal_bed_ratio"),  # Calculate ICU bed ratio

    round((sum("heart_disease") / count("patient_id")) * 100, 2).alias("patient_with_heart_disease"),  # Percentage of patients with heart disease

    round((sum("lung_disease") / count("patient_id")) * 100, 2).alias("patient_with_lung_disease")  # Percentage of patients with lung disease
)

    # Return the aggregated DataFrame
    return hospitals_agg_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## creating the patients aggregated facts 

# COMMAND ----------

@dlt.create_table(
  comment="The patients aggregated facts",
  table_properties={
    "wecure_deltaliv.quality": "gold",
    "pipelines.autoOptimize.managed": "true"
  }
)

def patients_agg_facts():

    # reading all the cleaned dlts into a df
    new_admissions_clean=dlt.read("new_admissions_clean")
    patients_df_clean=dlt.read("patients_df_clean")
    patient_history_clean=dlt.read("patient_history_clean")
    hospitals_clean=dlt.read("hospitals_clean")
    plans_clean=dlt.read("plans_clean")
    policies_new_clean=dlt.read("policies_new_clean")

    # reading the streaming dataframe 
    stream_df=dlt.read_stream("patient_device_stream_clean")

    # joining the required dataframes for creatation of facts

    df=policies.join(patients_df_clean,on="patient_id",how="right")


    # function to calculate the patients age from their date of birth
    def calculate_age(dob_column):
        return (
            datediff(current_date(), to_date(dob_column, "dd-MM-yyyy")) / 365
        ).cast("int")

    # Calculate the age of the patient based on the date of birth
    df = df.withColumn("age_of_patient", calculate_age(df["dob"]))

    # Join multiple DataFrames with inner joins
    df = df.join(patient_history_clean, on="patient_id", how="inner")
    df = df.join(new_admissions_clean, on="patient_id", how="inner")
    df = df.join(stream_df, on="patient_id", how="inner")
    df = df.join(hospitals_clean, hospitals_clean.hosp_id == df.hospital_id, how="inner")
    df = df.join(plans_clean, on="plan_id", how="inner")

    df=df.withColumn("active_policy_holder", when(col("policy_end_date")>col("d_o_a"),"True").otherwise("False"))

    # Calculate the 'tot' column based on various conditions
    df = df.withColumn('tot', col('dm') + col('htn') + col('cad') + col('prior_cmp') + col('ckd') + col('severe_anaemia') + col('stable_angina') + col('acs') + col('stemi') + col('atypical_chest_pain') + col('heart_failure') + col('hfref') + col('hfnef') + col('valvular') + col('chb') + col('sss') + col('aki') + col('cva_infract') + col('cva_bleed') + col('af') + col('vt'))

    # Calculate the 'risk' column based on multiple conditions
    df = df.withColumn("risk",
        when((col("smoking_") == 1) & (col("alcohol") == 1) & (col("tot") >= 2), "Critical")
        .when(((col("smoking_") == 1) | (col("alcohol") == 1)) & (col("tot") >= 2), "Very High")
        .when(col("tot") > 2, "High")
        .when(((col("smoking_") == 1) | (col("alcohol") == 1) | (col("tot") > 0)), "Medium")
        .when((col("smoking_") == 0) & (col("alcohol") == 0) & (col("tot") == 0), "Low")
        .otherwise("Unknown")
    )

# Calculate the 'insurance_coverage' column based on multiple conditions
    df = df.withColumn('insurance_coverage', (col('inpatient_care') & col('outpatient_care') &
                                              col('prescription_drugs') & col('mental_health_care') &
                                              col('dental_care') & col('vision_care')).cast('int'))

    # Group the DataFrame and calculate aggregated values
    df = df.groupBy("patient_id", "policy_number", "age_of_patient") \
            .agg(mode(col("active_policy_holder")).alias("active_policy_holder"),
                avg("tot").alias("total_medical_condition_of_patient"),
                 
                count("admission_id").alias("number_of_hospital_admissions"),

                mode("risk").alias("patient_risk"),

                sum("duration_of_stay").alias("total_hospital_days_of_patient"),

                avg("insurance_coverage").alias("insurance_coverage"),

                count("location").alias("no_of_cross_city_admissions"),
                
                round(avg("heart_rate_per_minute")).alias("avg_heart_rate"))

    # Return the aggregated DataFrame
    return df


# COMMAND ----------

# MAGIC %md
# MAGIC # thus all the facts were created in the aggregated tables and can be used by the downstream teams
