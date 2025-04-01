{{ config(
    materialized='incremental',
    on_schema_change='append_new_columns',
    unique_key=['film_id'],
    incremental_strategy='delete+insert'
    )}}

WITH CTE AS (

    SELECT
        sf.film_id 
        , sf.title 
        , sf.description 
        , sf.release_year 
        , sl."name" AS language
        , sf.rental_duration 
        , sf.rental_rate 
        , sf.length 
        , sf.replacement_cost 
        , sf.rating 
        , sf.special_features 
        , sf.fulltext 
        , sc."name" AS category
        , COALESCE(sf.src_data_ingestion_time, sfc.src_data_ingestion_time, 
				sl.src_data_ingestion_time, sc.src_data_ingestion_time) AS src_data_ingestion_time
    FROM 
	    {{ref('src_film')}} AS sf 
    LEFT JOIN {{ref('src_film_category')}} AS sfc ON sfc.film_id = sf.film_id 
    LEFT JOIN {{ref('src_language')}} AS sl ON sl.language_id = sf.language_id 
    LEFT JOIN {{ref('src_category')}} AS sc ON sc.category_id = sfc.category_id 
    WHERE 1=1
        AND sf.src_data_ingestion_time::date = CURRENT_DATE
        OR sfc.src_data_ingestion_time::date = CURRENT_DATE
        OR sl.src_data_ingestion_time::date = CURRENT_DATE
        OR sc.src_data_ingestion_time::date = CURRENT_DATE
),

dedupe AS (

    SELECT
        *
        , ROW_NUMBER() OVER(PARTITION BY film_id ORDER BY src_data_ingestion_time DESC) AS seq
    FROM
        CTE
),

final_result AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['film_id']) }} AS row_id
        , film_id
        , title
        , description
        , release_year
        , language
        , rental_duration
        , rental_rate
        , length
        , replacement_cost
        , rating
        , special_features
        , fulltext
        , category
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