create or replace table Keepcoding.ivr_details as
select
ivc.ivr_id  as calls_ivr_id, 
ivc.phone_number as calls_phone_number, 
ivc.ivr_result as calls_ivr_result, 
ivc.vdn_label as calls_vdn_label,
format_timestamp('%Y%m%d',ivc.start_date) as calls_start_date_id, 
ivc.start_date as calls_start_date, 
format_timestamp('%Y%m%d',ivc.end_date) as calls_end_date_id, 
ivc.end_date as calls_end_date, 
timestamp_diff(ivc.end_date, ivc.start_date, SECOND) as calls_total_duration, 
ivc.customer_segment as calls_customer_segment, 
ivc.ivr_language as calls_ivr_language, 
ivc.steps_module as calls_steps_module, 
ivc.module_aggregation as calls_module_aggregation, 
ivm.module_sequece  as module_sequence, 
ivm.module_name as module_name,
ivc.total_duration as module_duration,
ivm.module_result as module_result,
ivs.step_sequence as step_sequence, 
ivs.step_name as step_name,
ivs.step_result as step_result,
ivs.step_description_error as step_description_error,
ivs.document_type as document_type,
ivs.document_identification as document_identification,
ivs.customer_phone as customer_phone,
ivs.billing_account_id as billing_account_id,
from 
`Keepcoding.ivr_calls` as ivc
join 
`Keepcoding.ivr_modules` as ivm 
on 
ivc.ivr_id=ivm.ivr_id
join
`Keepcoding.ivr_steps` as ivs 
on 
ivm.ivr_id=ivs.ivr_id and ivm.module_sequece=ivs.module_sequece;