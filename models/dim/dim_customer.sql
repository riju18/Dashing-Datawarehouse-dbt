{{ config(
    materialized='incremental',
    on_schema_change='append_new_columns',
    unique_key=['customer_id', 'store_id', 'address_id'],
    incremental_strategy='delete+insert'
    )}}

WITH CTE AS (

    SELECT
        customer_id
        , store_id
        , address_id
        , {{ trim_col('first_name') }} AS first_name
        , {{ trim_col('last_name') }} AS last_name
        , {{ trim_col('email') }} AS email
        , src_data_ingestion_time
        , ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY src_data_ingestion_time DESC) AS seq
    FROM
        {{ref('src_customer')}}
    WHERE 1=1
        AND {{timestamp_to_date('src_data_ingestion_time')}} = CURRENT_DATE
),

final_result AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'store_id', 'address_id']) }} AS row_id
        , customer_id
        , store_id
        , address_id
        , first_name
        , last_name
        , email
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