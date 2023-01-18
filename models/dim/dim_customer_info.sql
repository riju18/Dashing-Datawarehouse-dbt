{#unique_key is used to replace old data with new data #}

{{
    config(
    materialized = 'incremental',
    unique_key = 'customer_id'
    )
 }}
with dim_customer_info as
(
    select
        customer_id
        , first_name
        , last_name
        , email
        , create_date as onboard_date
        , last_update as customer_last_update_date
        , customer_is_active
        , address
        , address2
        , city
        , district
        , country
        , postal_code
        , phone
        , current_timestamp as data_insertion_date
    from {{ ref('src_customer') }}
    )
select
    *
from dim_customer_info
{% if is_incremental() %}
    where 1=1
        and customer_last_update_date > (select max(customer_last_update_date) from {{ this }})
{% endif %}