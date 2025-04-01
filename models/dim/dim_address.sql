{{ config(
    materialized='incremental',
    on_schema_change='append_new_columns',
    unique_key=['address_id'],
    incremental_strategy='delete+insert'
    )}}

WITH CTE AS (

    SELECT
        sa.address_id 
        , sc2.country 
        , sc.city 
        , COALESCE(sa.address, sa.address2) AS address
        , sa.district 
        , sa.postal_code 
        , sa.phone 
        , COALESCE(sa.src_data_ingestion_time, sc.src_data_ingestion_time, sc2.src_data_ingestion_time) AS src_data_ingestion_time
    FROM
        {{ref('src_address')}} AS sa 
    LEFT JOIN {{ref('src_city')}} AS sc ON sc.city_id = sa.city_id
    LEFT JOIN {{ref('src_country')}} AS sc2 ON sc2.country_id = sc.country_id
    WHERE 1=1
        AND {{timestamp_to_date('sa.src_data_ingestion_time')}} = CURRENT_DATE
        OR {{timestamp_to_date('sc.src_data_ingestion_time')}} = CURRENT_DATE
        OR {{timestamp_to_date('sc2.src_data_ingestion_time')}} = CURRENT_DATE
),

dedupe AS (

    SELECT
        *
        , ROW_NUMBER() OVER(PARTITION BY address_id ORDER BY src_data_ingestion_time DESC) AS seq
    FROM
        CTE
),

final_result AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['address_id']) }} AS row_id
        , address_id
        , country
        , city
        , address
        , district
        , postal_code
        , phone
        , src_data_ingestion_time
        , user AS created_by
        , CURRENT_TIMESTAMP AS dim_data_ingestion_time
    FROM
        dedupe
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