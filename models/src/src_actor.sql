{{ config(
    materialized='incremental',
    on_schema_change='append_new_columns',
    partition_by={
        "field": "src_data_ingestion_time",
        "data_type": "timestamp",
        "granularity": "year"
        }
    )}}

with cte as (

    select
        {{ dbt_utils.generate_surrogate_key(['actor_id']) }} as row_id
        , *
        , user as created_by
        , current_timestamp as src_data_ingestion_time
    from
        {{source('dvdrental_raw_data', 'actor')}}
    where 1=1
        and last_update::date = current_date - 1
)

select
    *
from
    cte 

{% if is_incremental() %}

WHERE 1=1 
    AND last_update >= (SELECT max(last_update) FROM {{ this }})

{% endif %}