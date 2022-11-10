
{{ config(materialized = 'incremental',
    unique_key = 'time') }}

-- select 
--     * 
-- from 
--     "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."ORDERS"
-- WHERE
--     O_ORDERDATE <= '1998-07-15'

-- {% if is_incremental() %}
--     and O_ORDERDATE > (select max(d_date) from {{this}} )
-- {% endif %}


select 
    to_time(concat(T_HOUR::varchar, ':', T_MINUTE, ':', T_SECOND)) as time,
    * 
from "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF10TCL"."TIME_DIM"
where
    time < current_time
{% if is_incremental() %}
    and time > (select max(time) from {{this}} )
{% endif %}
order by 1 desc
