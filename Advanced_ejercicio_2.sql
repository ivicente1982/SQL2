create or replace table `Keepcoding.ivr_query1` as select
ivd.calls_ivr_id as ivr_id,
ivd.calls_phone_number as phone_number,
ivd.calls_ivr_result as ivr_result,
      CASE WHEN starts_with (ivd.calls_vdn_label, 'ATC') THEN 'FRONT'
            WHEN starts_with (ivd.calls_vdn_label, 'TECH') THEN 'TECH'
            WHEN starts_with (ivd.calls_vdn_label, 'ABSORPTION') THEN 'ABSORPTION'
            ELSE 'RESTO'
       END AS vdn_aggregation,
ivd.calls_start_date as start_date,
ivd.calls_end_date as end_date,
ivd.calls_total_duration as total_duration,
ivd.calls_customer_segment as customer_segment,
ivd.calls_ivr_language as ivr_language,
ivd.calls_steps_module as steps_module,
ivd.calls_module_aggregation as module_aggregation,
ivd.document_type as document_type,
ivd.document_identification as document_identification,
ivd.billing_account_id as billing_account_id,
if (ivd.module_name='AVERIA_MASIVA',1,0) AS MASIVA_LG,
IF (step_name = 'CUSTOMERINFOBYPHONE.TX' AND step_description_error='NULL',1,0) AS info_by_phone_lg,
IF (step_name='CUSTOMERINFOBYDNI.TX'AND step_description_error='NULL',1,0) AS info_by_dni_lg,
FROM `Keepcoding.ivr_details` as ivd;




CREATE OR REPLACE table `Keepcoding.ivr_query2` as
WITH Difllam AS (
  SELECT
    calls_ivr_id,
    calls_phone_number,
    calls_start_date,
    LAG(calls_start_date) OVER (PARTITION BY calls_phone_number ORDER BY calls_start_date) AS tiempo_anterior
  FROM
    `Keepcoding.ivr_details`)
SELECT
  calls_ivr_id,
  calls_phone_number,
  calls_start_date,
  tiempo_anterior,
IF(TIMESTAMP_DIFF(calls_start_date, tiempo_anterior, HOUR)<24,1,0) AS repeated_phone_24h,
FROM
  difllam
WHERE
  tiempo_anterior IS NOT NULL
ORDER BY
  calls_phone_number, calls_start_date;

CREATE OR REPLACE table `Keepcoding.ivr_query3` as
WITH Difllam AS (
  SELECT
    calls_ivr_id,
    calls_phone_number,
    calls_start_date,
    LAG(calls_start_date) OVER (PARTITION BY calls_phone_number ORDER BY calls_start_date DESC) AS tiempo_posterior
  FROM
    `Keepcoding.ivr_details`)
SELECT
  calls_ivr_id,
  calls_phone_number,
  calls_start_date,
  tiempo_posterior,
IF(TIMESTAMP_DIFF(calls_start_date, tiempo_posterior, HOUR)<24,1,0) AS cause_recall_24h,
FROM
  difllam
WHERE
  tiempo_posterior IS NOT NULL
ORDER BY
  calls_phone_number, calls_start_date;

select
ivd.calls_ivr_id as ivr_id,
ivd.calls_phone_number as phone_number,
ivd.calls_ivr_result as ivr_result,
      CASE WHEN starts_with (ivd.calls_vdn_label, 'ATC') THEN 'FRONT'
            WHEN starts_with (ivd.calls_vdn_label, 'TECH') THEN 'TECH'
            WHEN starts_with (ivd.calls_vdn_label, 'ABSORPTION') THEN 'ABSORPTION'
            ELSE 'RESTO'
       END AS vdn_aggregation,
ivd.calls_start_date as start_date,
ivd.calls_end_date as end_date,
ivd.calls_total_duration as total_duration,
ivd.calls_customer_segment as customer_segment,
ivd.calls_ivr_language as ivr_language,
ivd.calls_steps_module as steps_module,
ivd.calls_module_aggregation as module_aggregation,
ivd.document_type as document_type,
ivd.document_identification as document_identification,
ivd.billing_account_id as billing_account_id,
if (ivd.module_name='AVERIA_MASIVA',1,0) AS MASIVA_LG,
IF (step_name = 'CUSTOMERINFOBYPHONE.TX' AND step_description_error='NULL',1,0) AS info_by_phone_lg,
IF (step_name='CUSTOMERINFOBYDNI.TX'AND step_description_error='NULL',1,0) AS info_by_dni_lg,
ivq2.repeated_phone_24h as repeated_phone_24h,
ivq3.cause_recall_24h as cause_recall_24h 
FROM `Keepcoding.ivr_details` as ivd
join
`Keepcoding.ivr_query2` as ivq2
on
ivd.calls_ivr_id=ivq2.calls_ivr_id
join
`Keepcoding.ivr_query3`as ivq3
on
ivd.calls_ivr_id=ivq3.calls_ivr_id


