{{ config(
    materialized='incremental',
    on_schema_change='append_new_columns',
    unique_key=['actor_id', 'film_id'],
    incremental_strategy='delete+insert'
    )}}

WITH CTE AS (

    SELECT
        actor_id
        , film_id
        , last_update
        , ROW_NUMBER() OVER(PARTITION BY actor_id, film_id ORDER BY last_update DESC) AS seq
    FROM
        {{source('dvdrental_raw_data', 'film_actor')}}
    WHERE 1=1
        AND {{timestamp_to_date('last_update')}} = CURRENT_DATE - 1
),

final_result AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['actor_id', 'film_id']) }} AS row_id
        , actor_id
        , film_id
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