{{ config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='sync_all_columns'
) }}

  select distinct
  {{ dbt_utils.generate_surrogate_key(
      ['state_fips_code']
  ) }} as id  ,
state_fips_code,  
current_timestamp as _dbt_loaded_at
from  {{ref('maternal_opioid_use_hospital_stays') }} as maternal_opioid_use_hospital_stays
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where date(_dbt_loaded_at) > (select date(max(_dbt_loaded_at)) from {{ this }} )

{% endif %}


