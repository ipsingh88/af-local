{{ config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='sync_all_columns'
) }}

  select distinct
  {{ dbt_utils.generate_surrogate_key(
      ['time_period']
  ) }} as id  ,
time_period,  
time_period_date_start,				
time_period_date_end,
current_timestamp as _dbt_loaded_at
from  {{ref('maternal_opioid_use_hospital_stays') }} as maternal_opioid_use_hospital_stays
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where date(_dbt_loaded_at) > (select date(max(_dbt_loaded_at)) from {{ this }} )

{% endif %}


