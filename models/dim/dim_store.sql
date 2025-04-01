{{ config(
    materialized='incremental',
    on_schema_change='append_new_columns',
    unique_key=['store_id', 'manager_staff_id', 'address_id'],
    incremental_strategy='delete+insert'
    )}}

WITH CTE AS (

    SELECT
        store_id
        , manager_staff_id
        , address_id
        , src_data_ingestion_time
        , ROW_NUMBER() OVER(PARTITION BY store_id, manager_staff_id, address_id ORDER BY src_data_ingestion_time DESC) AS seq
    FROM
        {{ref('src_store')}}
    WHERE 1=1
        AND {{timestamp_to_date('src_data_ingestion_time')}} = CURRENT_DATE
),

final_result AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['store_id', 'manager_staff_id', 'address_id']) }} AS row_id
        , store_id
        , manager_staff_id
        , address_id
        , src_data_ingestion_time
        , user AS created_by
        , CURRENT_TIMESTAMP AS dim_data_ingestion_time
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
    AND src_data_ingestion_time > (SELECT MAX(src_data_ingestion_time) FROM {{ this }})

{% endif %}