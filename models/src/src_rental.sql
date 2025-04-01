{{ config(
    materialized='incremental',
    on_schema_change='append_new_columns',
    unique_key=['rental_id', 'inventory_id', 'customer_id', 'staff_id'],
    incremental_strategy='delete+insert'
    )}}

WITH CTE AS (

    SELECT
        rental_id
        , inventory_id
        , customer_id
        , staff_id
        , rental_date
        , return_date
        , last_update
        , ROW_NUMBER() OVER(PARTITION BY rental_id, inventory_id, customer_id, staff_id ORDER BY last_update DESC) AS seq
    FROM
        {{source('dvdrental_raw_data', 'rental')}}
    WHERE 1=1
        AND {{timestamp_to_date('last_update')}} = CURRENT_DATE - 1
),

final_result AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['rental_id', 'inventory_id', 'customer_id', 'staff_id']) }} AS row_id
        , rental_id
        , inventory_id
        , customer_id
        , staff_id
        , rental_date
        , return_date
        , last_update
        , user AS created_by
        , CURRENT_TIMESTAMP AS src_data_ingestion_time
    FROM
        CTE
    WHERE 1=1
        and seq = 1
)

SELECT
    *
FROM
    final_result 

{% if is_incremental() %}

WHERE 1=1 
    AND last_update > (SELECT MAX(last_update) FROM {{ this }})

{% endif %}