{{ config(
    materialized='incremental',
    on_schema_change='append_new_columns',
    unique_key=['payment_id', 'customer_id', 'staff_id', 'rental_id'],
    incremental_strategy='delete+insert'
    )}}

WITH CTE AS (

    SELECT
        payment_id
        , customer_id
        , staff_id
        , rental_id
        , amount
        , payment_date
        , ROW_NUMBER() OVER(PARTITION BY payment_id, customer_id, staff_id, rental_id ORDER BY payment_date DESC) AS seq
    FROM
        {{source('dvdrental_raw_data', 'payment')}}
    WHERE 1=1
        AND {{timestamp_to_date('payment_date')}} = CURRENT_DATE - 1
),

final_result AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['payment_id', 'customer_id', 'staff_id', 'rental_id']) }} AS row_id
        , payment_id
        , customer_id
        , staff_id
        , rental_id
        , amount
        , payment_date
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
    AND payment_date > (SELECT MAX(payment_date) FROM {{ this }})

{% endif %}