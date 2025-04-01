-- timestamp to date
{% macro timestamp_to_date(col) %}
    {{col}}::date
{% endmacro %}