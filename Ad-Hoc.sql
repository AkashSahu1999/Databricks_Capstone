

-- Question 1

SELECT COUNT(DISTINCT patient_id) AS count_of_patients
FROM policies_new
WHERE daily_premium > (SELECT AVG(daily_premium) FROM policies_new);


-- Question 2
-- Part 1

SELECT
    AVG(CAST(p.inpatient_care AS INT)) AS Inpatient_AvgCoverage,
    AVG(CAST(p.outpatient_care AS INT)) AS Outpatient_AvgCoverage,
    AVG(CAST(p.prescription_drugs AS INT)) AS Prescription_AvgCoverage,
    AVG(CAST(p.mental_health_care AS INT)) AS MentalHealth_AvgCoverage,
    AVG(CAST(p.dental_care AS INT)) AS Dental_AvgCoverage,
    AVG(CAST(p.vision_care AS INT)) AS Vision_AvgCoverage
FROM
    plans p

--Part 2

SELECT
    p.plan_id AS plan_type,
    AVG(po.daily_premium) AS DailyPremium_Avg,
    SUM(po.daily_premium * DATEDIFF(po.policy_end_date, po.policy_start_date) + 1) / DATEDIFF(MAX(po.policy_end_date), MIN(po.policy_start_date)) AS AverageAnnualPremium
FROM
    plans p
JOIN
    policies_new po
ON
    p.plan_id = po.plan_id
GROUP BY
    p.plan_id;

--Question 3

SELECT CONCAT('BR-0', 
    SUBSTRING(hosp_id, LENGTH(hosp_id) - 3, 4)) AS branch_id, 
    name as hospital_name,sum(dm) as dm_true, 
    sum(htn) as htn_true, sum(case when dm=1 and htn = 1 then 1 else 0 end) as both,
    sum(case when dm=0 and htn = 0 then 1 else 0 end) as neither
from hospitals h
join new_admissions na on na.hospital_id = h.hosp_id
join patients p on p.patient_id=na.patient_id
join patient_history ph on ph.patient_id=p.patient_id
GROUP BY branch_id,hospital_name

--/Question4/

SELECT
    h.hosp_id AS branch_id,
    h.name AS hospital_name,
    COALESCE(AVG(YEAR(CURRENT_DATE()) - YEAR(TO_DATE(p.dob, 'dd-MM-yyyy'))), 0) AS avg_patient_age,
    COALESCE(COUNT(na.admission_id), 0) AS total_admissions,
    CASE
        WHEN COALESCE(AVG(YEAR(CURRENT_DATE()) - YEAR(TO_DATE(p.dob, 'dd-MM-yyyy'))), 0) >= 50 OR COALESCE(COUNT(na.admission_id), 0) >= 1000 THEN 'Tier 1'
        WHEN COALESCE(AVG(YEAR(CURRENT_DATE()) - YEAR(TO_DATE(p.dob, 'dd-MM-yyyy'))), 0) BETWEEN 40 AND 49 OR COALESCE(COUNT(na.admission_id), 0) BETWEEN 500 AND 999 THEN 'Tier 2'
        ELSE 'Tier 3'
    END AS branch_tier
FROM
    hospitals h
LEFT JOIN
    new_admissions na ON h.hosp_id = na.hospital_id
LEFT JOIN
    patients p ON na.patient_id = p.patient_id
GROUP BY
    h.hosp_id, h.name;

/*Question 5*/

SELECT
    pc.plan_id,
    YEAR(TO_DATE(nac.d_o_a, 'dd-MM-yyyy')) AS year,
    MONTH(TO_DATE(nac.d_o_a, 'dd-MM-yyyy')) AS month,
    COUNT(nac.admission_id) AS admission_count
FROM
    plans pc
JOIN
    policies_new pnc ON pnc.plan_id = pc.plan_id
JOIN
    patients pdc ON pdc.patient_id = pnc.patient_id
JOIN
    new_admissions nac ON nac.patient_id = pdc.patient_id
GROUP BY
    pc.plan_id,
    YEAR(TO_DATE(nac.d_o_a, 'dd-MM-yyyy')),
    MONTH(TO_DATE(nac.d_o_a, 'dd-MM-yyyy'));

/*Question 6*/

WITH RankedPolicies AS (
  SELECT
    policy_number,
    patient_id,
    policy_start_date,
    policy_end_date,
    plan_id,
    ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY policy_start_date) AS rn
  FROM
    policies_new
)
SELECT
  p1.patient_id,
  p1.plan_id AS Previous_Policy,
  p2.plan_id AS Current_Policy
FROM
  RankedPolicies p1
JOIN
  RankedPolicies p2
ON
  p1.patient_id = p2.patient_id
  AND p1.rn = p2.rn - 1
  AND p1.plan_id <> p2.plan_id
ORDER BY
  p1.patient_id, p1.policy_start_date;


-- /Question 7/


WITH cte AS (
  SELECT *
  FROM patient_history ph
  JOIN patients p
  ON ph.patient_id = p.patient_id
),
cte2 AS (
  SELECT *, DATEDIFF(year, to_date(dob, 'dd-MM-yyyy'), current_date) AS age
  FROM cte
),
cte3 AS (
  SELECT *,
    CASE
      WHEN age < 30 THEN 'Under30'
      WHEN age >= 30 AND age < 35 THEN '30-35'
      WHEN age >= 35 AND age < 40 THEN '35-40'
      WHEN age >= 40 AND age < 45 THEN '40-45'
      WHEN age >= 45 AND age < 50 THEN '45-50'
      WHEN age >= 50 AND age < 55 THEN '50-55'
      WHEN age >= 55 AND age < 60 THEN '55-60'
    END AS age_group
  FROM cte2
),
cte4 AS (
  SELECT
    age_group,
    SUM(CASE WHEN dm = 1 THEN 1 ELSE 0 END) AS diabetes,
    SUM(CASE WHEN htn = 1 THEN 1 ELSE 0 END) AS hypertension,
    SUM(CASE WHEN cad = 1 THEN 1 ELSE 0 END) AS cad
  FROM cte3
  GROUP BY age_group
)
SELECT
  age_group,
  CASE
    WHEN diabetes >= hypertension AND diabetes >= cad THEN 'diabetes'
    WHEN cad >= hypertension AND cad >= diabetes THEN 'cad'
    ELSE 'hypertension'
  END AS most_common_condition
FROM cte4;
