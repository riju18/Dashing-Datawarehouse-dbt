-- trim and lower a col val
{% macro trim_col(col) %}
    TRIM({{col}})
{% endmacro %}