SELECT
    first_name
    ,last_name
FROM
    {{ ref('dim_actor') }}
WHERE 1=1
    AND (first_name = '' OR first_name = ' ')
    AND (last_name = '' OR last_name = ' ')