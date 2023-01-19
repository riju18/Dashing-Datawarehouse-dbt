{% snapshot scd_dim_actor_info %}

    {{
        config(
            target_schema="dashingdvdrental",
            unique_key="actor_id",
            strategy="timestamp",
            updated_at="actor_last_update",
            invalidate_hard_deletes=True
        )
     }}

select * from {{ ref('dim_actor_info') }}

{% endsnapshot %}