with fact_customer_by_district
as(
    select
        district,
        count(1) as total_customer
    from
        {{ ref('src_customer') }}
    group by
        1
)
select * from fact_customer_by_district
