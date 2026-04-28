{% macro calc_weeks_of_stock(stock, sales) %}
    ({{ stock }} / nullif({{ sales }}, 0))
{% endmacro %}
