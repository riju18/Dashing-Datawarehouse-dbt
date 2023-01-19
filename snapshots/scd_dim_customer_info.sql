{% snapshot scd_dim_customer_info %}

    {{
        config(
            target_schema="dashingdvdrental",
            unique_key="customer_id",
            strategy="timestamp",
            updated_at="customer_last_update_date",
            invalidate_hard_deletes=True
        )
     }}

select * from {{ ref('dim_customer_info') }}

{% endsnapshot %}