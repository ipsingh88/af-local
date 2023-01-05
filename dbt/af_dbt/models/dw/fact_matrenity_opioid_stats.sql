{{ config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='sync_all_columns'
) }}

  select 
  {{ dbt_utils.generate_surrogate_key(
      ['dim_county.id','dim_state.id','dim_time_period.id']
  ) }} as id  ,
dim_county.id as dim_county_id,  
dim_state.id as dim_state_id,				
dim_time_period.id as dim_time_period_id,
type_of_count,		
count_of_maternal_stays_with,
type_of_rate,
rate_of_maternal_stays_with,
current_timestamp as _dbt_loaded_at
from  {{ref('maternal_opioid_use_hospital_stays') }} as maternal_opioid_use_hospital_stays
left join {{ref('dim_county') }} as dim_county
on dim_county.fips_county_code = maternal_opioid_use_hospital_stays.fips_county_code
left join {{ref('dim_state') }} as dim_state
on  maternal_opioid_use_hospital_stays.state_fips_code = dim_state.state_fips_code
left join {{ref('dim_time_period') }} as dim_time_period
on  maternal_opioid_use_hospital_stays.time_period = dim_time_period.time_period
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where date(least(maternal_opioid_use_hospital_stays._dbt_loaded_at, dim_county._dbt_loaded_at,dim_state._dbt_loaded_at, dim_time_period._dbt_loaded_at )) > (select date(max(_dbt_loaded_at)) from {{ this }} )

{% endif %}


