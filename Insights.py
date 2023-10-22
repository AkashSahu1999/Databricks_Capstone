# Databricks notebook source
hospitals_df=spark.read.format("delta").load("dbfs:/pipelines/ca8f94d8-01e2-42f4-a3e6-196b8c28ef94/tables/hospitals_clean")

new_admissions_df=spark.read.format("delta").load("dbfs:/pipelines/ca8f94d8-01e2-42f4-a3e6-196b8c28ef94/tables/new_admissions_clean")

patient_history_df=spark.read.format("delta").load("dbfs:/pipelines/ca8f94d8-01e2-42f4-a3e6-196b8c28ef94/tables/patient_history_clean")

patients_df=spark.read.format("delta").load("dbfs:/pipelines/ca8f94d8-01e2-42f4-a3e6-196b8c28ef94/tables/patients_df_clean")

plans_df=spark.read.format("delta").load("dbfs:/pipelines/ca8f94d8-01e2-42f4-a3e6-196b8c28ef94/tables/plans_clean")

policies_new_df=spark.read.format("delta").load("dbfs:/pipelines/ca8f94d8-01e2-42f4-a3e6-196b8c28ef94/tables/policies_new_clean")

 

hospitals_df.createOrReplaceTempView("hospitals")

new_admissions_df.createOrReplaceTempView("new_admissions")

patient_history_df.createOrReplaceTempView("patient_history")

plans_df.createOrReplaceTempView("plans")

patients_df.createOrReplaceTempView("patients")

policies_new_df.createOrReplaceTempView("policies_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- # Count of Hospitals by Sector:
# MAGIC SELECT sector, COUNT(*) AS hospital_count
# MAGIC FROM hospitals
# MAGIC GROUP BY sector;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Average Number of Licensed Beds by State:
# MAGIC SELECT state, AVG(num_licensed_beds) AS avg_licensed_beds
# MAGIC FROM hospitals
# MAGIC GROUP BY state;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Percentage of Smokers among Patients:
# MAGIC SELECT
# MAGIC     SUM(CASE WHEN smoking_ = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS percentage_smokers
# MAGIC FROM patient_history;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 5 Hospitals with the Most Admissions:
# MAGIC SELECT hospital_id, COUNT(*) AS admission_count
# MAGIC FROM new_admissions
# MAGIC GROUP BY hospital_id
# MAGIC ORDER BY admission_count DESC
# MAGIC LIMIT 5;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Percentage of Patients with Diabetes and Hypertension:
# MAGIC SELECT
# MAGIC     SUM(CASE WHEN dm = 1 AND htn = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS percentage_diabetes_and_hypertension
# MAGIC FROM patient_history;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Average Daily Premium by Insurance Plan:
# MAGIC SELECT plan_id, AVG(daily_premium) AS avg_daily_premium
# MAGIC FROM policies_new
# MAGIC GROUP BY plan_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Percentage of Patients with Heart Failure:
# MAGIC SELECT
# MAGIC     SUM(CASE WHEN heart_failure = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS percentage_heart_failure
# MAGIC FROM patient_history;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Total Insurance Coverage by Plan:
# MAGIC SELECT plan_id, SUM(insurance_coverage) AS total_coverage
# MAGIC FROM policies_new
# MAGIC GROUP BY plan_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --Determine whether patients with different insurance plans have varying outcomes (e.g., readmission rates or mortality rates).
# MAGIC
# MAGIC SELECT
# MAGIC     p.plan_id,
# MAGIC     COUNT(na.admission_id) AS total_admissions,
# MAGIC     AVG(na.duration_of_stay) AS avg_duration_of_stay
# MAGIC FROM
# MAGIC     new_admissions AS na
# MAGIC JOIN
# MAGIC     policies_new AS p
# MAGIC ON
# MAGIC     na.patient_id = p.patient_id
# MAGIC GROUP BY
# MAGIC     p.plan_id;

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC -- Assess the efficiency of resource allocation by analyzing the usage of ICU beds and ventilators in hospitals.
# MAGIC
# MAGIC SELECT
# MAGIC     h.name AS hospital_name,
# MAGIC     (COUNT(DISTINCT na.admission_id) / h.num_icu_beds) AS icu_bed_utilization_rate,
# MAGIC     (COUNT(DISTINCT na.admission_id) / h.ventilators_available) AS ventilator_utilization_rate
# MAGIC FROM
# MAGIC     hospitals AS h
# MAGIC LEFT JOIN
# MAGIC     new_admissions AS na
# MAGIC ON
# MAGIC     h.hosp_id = na.hospital_id
# MAGIC GROUP BY
# MAGIC     h.name, h.num_icu_beds, h.ventilators_available;

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC -- Analyze the demographics of patients admitted to hospitals, segmented by the sector (public or government) of the hospital they were admitted to.
# MAGIC
# MAGIC SELECT
# MAGIC     h.sector,
# MAGIC     AVG(YEAR(CURRENT_DATE()) - YEAR(to_date(p.dob, 'dd-MM-yyyy'))) AS avg_age,
# MAGIC     COUNT(p.patient_id) AS total_patients
# MAGIC FROM
# MAGIC     new_admissions AS na
# MAGIC JOIN
# MAGIC     patients AS p
# MAGIC ON
# MAGIC     na.patient_id = p.patient_id
# MAGIC JOIN
# MAGIC     hospitals AS h
# MAGIC ON
# MAGIC     na.hospital_id = h.hosp_id
# MAGIC GROUP BY
# MAGIC     h.sector;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analyze the popularity of different insurance plans among patients using data from the plans and policies_new dataframes.
# MAGIC SELECT
# MAGIC     p.payment_type,
# MAGIC     COUNT(DISTINCT pn.policy_number) AS policy_count,
# MAGIC     (COUNT(DISTINCT pn.policy_number) / (SELECT COUNT(DISTINCT policy_number) FROM policies_new)) AS percentage_of_total
# MAGIC FROM
# MAGIC     plans AS p
# MAGIC LEFT JOIN
# MAGIC     policies_new AS pn
# MAGIC ON
# MAGIC     p.plan_id = pn.plan_id
# MAGIC GROUP BY
# MAGIC     p.payment_type;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Identify patients who have a long-term relationship with specific hospitals or providers using data from the hospitals, new_admissions, and patient dataframes
# MAGIC SELECT
# MAGIC     h.name AS hospital_name,
# MAGIC     p.full_name AS provider_name,
# MAGIC     COUNT(DISTINCT na.admission_id) AS total_admissions,
# MAGIC     AVG(na.duration_of_stay) AS avg_duration_of_stay
# MAGIC FROM
# MAGIC     hospitals AS h
# MAGIC JOIN
# MAGIC     new_admissions AS na
# MAGIC ON
# MAGIC     h.hosp_id = na.hospital_id
# MAGIC JOIN
# MAGIC     patients AS p
# MAGIC ON
# MAGIC     na.patient_id = p.patient_id
# MAGIC GROUP BY
# MAGIC     h.name, p.full_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC /* no of patients per hospitals */
# MAGIC select name as hospital_name, 
# MAGIC count(pdc.patient_id) as count_of_patients 
# MAGIC from patients pdc 
# MAGIC join new_admissions ndc 
# MAGIC on ndc.patient_id=pdc.patient_id
# MAGIC join hospitals h on h.hosp_id=ndc.hospital_id
# MAGIC group by hospital_name
# MAGIC order by count(pdc.patient_id) desc

# COMMAND ----------

# MAGIC %sql
# MAGIC /* highest no.of emergency patients on a particular day*/
# MAGIC select d_o_a as date, count(patient_id) as no_of_patients
# MAGIC from new_admissions
# MAGIC where type_of_admission_emergency_or_opd == "E"
# MAGIC group by date
# MAGIC order by count(patient_id) desc
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC /* geographical distribution of patients*/
# MAGIC select state, location, count(pdc.patient_id) 
# MAGIC from patients pdc 
# MAGIC join new_admissions ndc 
# MAGIC on ndc.patient_id=pdc.patient_id
# MAGIC join hospitals h on h.hosp_id=ndc.hospital_id
# MAGIC group by state, location 
# MAGIC order by count(pdc.patient_id) desc

# COMMAND ----------

# MAGIC %sql
# MAGIC /* year wise month wise no of emergency cases that lead to death*/
# MAGIC select YEAR(TO_DATE(ndc.d_o_a, 'dd-MM-yyyy')) AS year,MONTH(TO_DATE(ndc.d_o_a, 'dd-MM-yyyy')) AS month, count(pdc.patient_id) as count_of_caes from patients pdc join new_admissions ndc on ndc.patient_id=pdc.patient_id
# MAGIC join hospitals h on h.hosp_id=ndc.hospital_id
# MAGIC where type_of_admission_emergency_or_opd=="E" and outcome=="EXPIRY"
# MAGIC group by year,month

# COMMAND ----------

# MAGIC %sql
# MAGIC /*average duration of stay in hospitals in case of emergency to get discharged*/
# MAGIC select name as hospital_name, avg(duration_of_stay) as avg_stay from patients pdc join new_admissions ndc on ndc.patient_id=pdc.patient_id
# MAGIC join hospitals h on h.hosp_id=ndc.hospital_id
# MAGIC where outcome=="DISCHARGE" and type_of_admission_emergency_or_opd=="E"
# MAGIC group by hospital_name
# MAGIC order by avg(duration_of_stay)

# COMMAND ----------

# MAGIC %sql
# MAGIC /*average duration of stay in hospitals in case of emergency to get discharged*/
# MAGIC select name as hospital_name, avg(duration_of_stay) as avg_stay from patients pdc join new_admissions ndc on ndc.patient_id=pdc.patient_id
# MAGIC join hospitals h on h.hosp_id=ndc.hospital_id
# MAGIC where outcome=="DISCHARGE" and type_of_admission_emergency_or_opd=="E"
# MAGIC group by hospital_name
# MAGIC order by avg(duration_of_stay)

# COMMAND ----------

# MAGIC %sql
# MAGIC /*average duration of stay in hospitals in case of emergency to get discharged*/
# MAGIC select name as hospital_name, avg(duration_of_stay) as avg_stay from patients pdc join new_admissions ndc on ndc.patient_id=pdc.patient_id
# MAGIC join hospitals h on h.hosp_id=ndc.hospital_id
# MAGIC where outcome=="DISCHARGE" and type_of_admission_emergency_or_opd=="E"
# MAGIC group by hospital_name
# MAGIC order by avg(duration_of_stay)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC /* state wise location wise total licensed beds*/
# MAGIC select state, location , sum(num_licensed_beds) as total_beds
# MAGIC from hospitals
# MAGIC group by state, location
# MAGIC order by sum(num_licensed_beds) desc
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC /* state wise location wise count of government hospitals*/
# MAGIC select state,location,count(sector) as no_of_gov_hospitals from hospitals
# MAGIC where sector=="Public/Government"
# MAGIC group by state,location
# MAGIC order by count(sector) desc
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC /*state wise location wise count of hospitals*/
# MAGIC select location,state, count(hosp_id) as count_of_hospitals from hospitals
# MAGIC group by location,state
# MAGIC order by count(hosp_id) desc
