{{ config(
    materialized='incremental',
    on_schema_change='append_new_columns',
    unique_key=['actor_id', 'release_year', 'language', 'rating'],
    incremental_strategy='delete+insert'
    )}}

WITH CTE AS (

    SELECT 
        sfa.actor_id 
        , df.release_year 
        , df."language" 
        , df.rating  
        , COUNT(DISTINCT df.film_id) AS total_film
    FROM 
        {{ref('src_film_actor')}} AS sfa
    LEFT JOIN {{ref('dim_film')}} AS df ON df.film_id = sfa.film_id 
    WHERE 1=1
        AND sfa.src_data_ingestion_time::date = CURRENT_DATE
        OR df.src_data_ingestion_time::date = CURRENT_DATE
    GROUP BY 1, 2, 3, 4
),

final_result AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['actor_id', 'release_year', 'language', 'rating']) }} AS row_id
        , *
        , user AS created_by
        , CURRENT_TIMESTAMP AS fact_data_ingestion_time
    FROM
        CTE
)

SELECT
    *
FROM
    final_result