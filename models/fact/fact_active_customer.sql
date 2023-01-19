with fact_active_customer
as(
    select
        customer_is_active,
        count(1) as total_customer
    from
        {{ ref('src_customer') }}
    group by
        1
)
select * from fact_active_customer