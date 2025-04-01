{{ config(
    materialized='incremental',
    on_schema_change='append_new_columns',
    unique_key=['inventory_id', 'film_id', 'store_id'],
    incremental_strategy='delete+insert'
    )}}

WITH CTE AS (

    SELECT
        inventory_id
        , film_id
        , store_id
        , ROW_NUMBER() OVER(PARTITION BY inventory_id, film_id, store_id ORDER BY last_update DESC) AS seq
    FROM
        {{source('dvdrental_raw_data', 'inventory')}}
    WHERE 1=1
        AND last_update::date = CURRENT_DATE - 1
),

final_result AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['inventory_id', 'film_id', 'store_id']) }} AS row_id
        , inventory_id
        , film_id
        , store_id
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