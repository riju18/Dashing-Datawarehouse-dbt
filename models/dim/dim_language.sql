{{ config(
    materialized='incremental',
    on_schema_change='append_new_columns',
    unique_key=['language_id'],
    incremental_strategy='delete+insert'
    )}}

WITH CTE AS (

    SELECT
        language_id
        , TRIM(name) AS language
        , src_data_ingestion_time
        , ROW_NUMBER() OVER(PARTITION BY language_id ORDER BY src_data_ingestion_time DESC) AS seq
    FROM
        {{ref('src_language')}}
    WHERE 1=1
        AND src_data_ingestion_time::date = CURRENT_DATE
),

final_result AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['language_id']) }} AS row_id
        , language_id
        , language
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