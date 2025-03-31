{{ config(
    materialized='incremental',
    on_schema_change='append_new_columns',
    partition_by={
        "field": "src_data_ingestion_time",
        "data_type": "timestamp",
        "granularity": "year"
        }
    )}}

WITH CTE AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['rental_id', 'customer_id', 'staff_id', 'inventory_id']) }} AS row_id
        , *
        , user AS created_by
        , CURRENT_TIMESTAMP AS src_data_ingestion_time
    FROM
        {{source('dvdrental_raw_data', 'rental')}}
    WHERE 1=1
        AND last_update::date = CURRENT_DATE - 1
)

SELECT
    *
FROM
    cte 

{% if is_incremental() %}

WHERE 1=1 
    AND last_update > (SELECT MAX(last_update) FROM {{ this }})

{% endif %}