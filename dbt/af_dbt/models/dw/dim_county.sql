{{ config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='sync_all_columns'
) }}

  select distinct
  {{ dbt_utils.generate_surrogate_key(
      ['fips_county_code']
  ) }} as id  ,
fips_county_code,  
county_name,				
county_fips_code,
county_code_number,
latitude_longitude,
latitude_longitude_geom,		
geocoded_column_type,
geocoded_column_coordinates,
geocoded_column_coordinates_geom	,
current_timestamp as _dbt_loaded_at
from  {{ref('maternal_opioid_use_hospital_stays') }} as maternal_opioid_use_hospital_stays
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where date(_dbt_loaded_at) > (select date(max(_dbt_loaded_at)) from {{ this }} )

{% endif %}


