{% test equal_count(model, column_name, source_model, source_column) %}

WITH dimension_count AS (
    SELECT 
        COUNT({{ column_name }}) AS dim_count 
    FROM 
        {{ model }}
),

source_count AS (
    SELECT 
        COUNT(DISTINCT {{source_column}}) AS source_count 
    FROM 
        {{source_model}}
)

SELECT 
	1
FROM 
    source_count AS sc
JOIN dimension_count AS dc ON sc.source_count != dc.dim_count

{% endtest %}