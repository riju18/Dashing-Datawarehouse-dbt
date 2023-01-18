with fact_day_wise_onboard_customer
as(
    select
        onboard_date,
        count(1) as total_onboarded_customer
    from
        {{ ref('dim_customer_info') }}
    group by
        1
)
select * from fact_day_wise_onboard_customer
