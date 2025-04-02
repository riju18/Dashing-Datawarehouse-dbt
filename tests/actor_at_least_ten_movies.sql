SELECT
	actor_id 
FROM
	{{ref('fact_actor_total_film')}}
GROUP BY 1
HAVING SUM(total_film) < 10