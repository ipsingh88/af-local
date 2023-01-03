{{ config(
    materialized='incremental',
    unique_key='fips_county_code',
    on_schema_change='sync_all_columns'
) }}

with src as 
(
  select 
county_name ,		
cast(fips_county_code as integer),		
cast(state_fips_code as integer),
cast(county_fips_code as integer),
cast(county_code_number as integer),
latitude_longitude,
ST_GeomFromText(replace(replace(latitude_longitude ,'(','POINT(') ,',',''),4326) latitude_longitude_geom,		
time_period ,
cast(time_period_date_start as timestamp),
cast(time_period_date_end as timestamp),		
cast(count_of_maternal_stays_with  as integer),
type_of_count,
cast(rate_of_maternal_stays_with as double precision),	
type_of_rate,
geocoded_column_type,
geocoded_column_coordinates,
ST_GeomFromText( replace(replace(replace(geocoded_column_coordinates, '{',geocoded_column_type||'('),'}',')') ,',',' ') ,4326) geocoded_column_coordinates_geom	,
cast(computed_region_nmsq_hqvv as integer),
cast(computed_region_d3gw_znnf as integer),
cast(computed_region_amqz_jbr4 as integer),
cast(computed_region_r6rf_p9et as integer),
cast(computed_region_rayf_jjgk as integer),
loaded_at,
current_timestamp as _dbt_loaded_at,
md5(ROW(county_name, fips_county_code, state_fips_code, county_fips_code, county_code_number, latitude_longitude, time_period, time_period_date_start, time_period_date_end, count_of_maternal_stays_with, type_of_rate, geocoded_column_type, geocoded_column_coordinates, computed_region_nmsq_hqvv, computed_region_d3gw_znnf, computed_region_amqz_jbr4, computed_region_r6rf_p9et, computed_region_rayf_jjgk)::TEXT) col_hash
from  {{source('af_local_raw', 'maternal_opioid_use_hospital_stays') }} as maternal_opioid_use_hospital_stays
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where date(loaded_at) >= (select date(max(loaded_at)) from {{ this }} )

{% endif %}
)
select * from 
src
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where not exists (select 'x' from {{ this }} as t where t.col_hash = src.col_hash )

{% endif %}


