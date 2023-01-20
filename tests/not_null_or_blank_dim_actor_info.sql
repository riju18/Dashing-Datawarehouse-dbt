select
    first_name,
    last_name
from
    {{ ref('dim_actor_info') }}
where 1=1
    and (first_name = '' or first_name = ' ')
    and (last_name = '' or last_name = ' ')