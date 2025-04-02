SELECT
	film_id 
FROM
	{{ref('dim_film')}}
WHERE release_year < 2006