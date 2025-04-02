SELECT
    customer_id
FROM
    {{ ref('dim_customer') }}
WHERE 1=1
    AND (first_name = '' OR first_name = ' ')
    OR (last_name = '' OR last_name = ' ')