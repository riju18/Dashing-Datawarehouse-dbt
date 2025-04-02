{% snapshot scd_fact_actor_total_film %}

{{
    config(
      unique_key='row_id',
      updated_at='fact_data_ingestion_time',
    )
}}

SELECT
    * 
FROM 
    {{ ref('fact_actor_total_film') }}

{% endsnapshot %}