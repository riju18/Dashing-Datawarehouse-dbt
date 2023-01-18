with actor_film_info as (
select
	aa.actor_id
	, ff.language_id
	, aa.first_name
	, aa.last_name
    , ff.film_id
	, ff.title as film_name
	, ff.description as film_description
	, ff.release_year as film_release_year
	, ff.rental_duration as rental_duration_in_day
	, ceil(ff.rental_rate) as rental_rate_in_dollar
	, ff.length as film_duration_in_min
	, ceil(ff.replacement_cost) as replacement_cost
	, ff.rating
	, ff.special_features
	, ff.fulltext as extra_info
from public.actor aa
left join public.film_actor fa on fa.actor_id = aa.actor_id
left join public.film ff on ff.film_id = fa.film_id
order by aa.actor_id
),

film_language as (
	select
		language_id
		, name as film_language
	from public."language"
),

actor_final_film_info as (
	select
		aa.*
		, fl.film_language
	from actor_film_info aa
	left join film_language fl on aa.language_id = fl.language_id
),

actor_film_final as (
select
	*
from actor_final_film_info
)

select * from actor_film_final