with fact_day_wise_onboard_customer
as(
    select
        create_date,
        count(1) as total_onboarded_customer
    from
        {{ ref('src_customer') }}
    group by
        1
)
select * from fact_day_wise_onboard_customer
