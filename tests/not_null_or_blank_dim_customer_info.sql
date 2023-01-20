select
    customer_is_active,
    first_name,
    last_name
from
    {{ ref('dim_customer_info') }}
where 1=1
    and (customer_is_active = '' or customer_is_active = ' ')
    and (first_name = '' or first_name = ' ')
    and (last_name = '' or last_name = ' ')