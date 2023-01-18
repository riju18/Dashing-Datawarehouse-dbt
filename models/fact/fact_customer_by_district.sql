with fact_customer_by_district
as(
    select
        district,
        count(1) as total_customer
    from
        {{ ref('dim_customer_info') }}
    group by
        1
)
select * from fact_customer_by_district
