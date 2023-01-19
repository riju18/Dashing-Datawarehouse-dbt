{# actor_id is used to replace old data with new data #}

{{
    config(
    materialized = 'incremental',
    unique_key = 'actor_id'
    )
 }}
with dim_actor_info as
(
	select
		t1.actor_id,
		t1.first_name,
		t1.last_name,
		t1.actor_last_update
	from
	(
		select
			actor_id ,
			first_name,
			last_name,
			actor_last_update,
			row_number () over(partition by actor_id) as seq  {# distinct actor info #}
		from
			{{ ref('src_actor_film') }}
	) t1
	where 1=1
		and t1.seq = 1
)
select
    *
from dim_actor_info
{% if is_incremental() %}
    where 1=1
        and actor_last_update > (select max(actor_last_update) from {{ this }})
{% endif %}