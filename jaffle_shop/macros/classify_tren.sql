{% macro classify_trend(metric, partition_by, order_by) %}
    case
        when {{ metric }} > lag({{ metric }}) over (partition by {{ partition_by }} order by {{ order_by }})
            then 'creciente'
        when {{ metric }} < lag({{ metric }}) over (partition by {{ partition_by }} order by {{ order_by }})
            then 'decreciente'
        else 'estable'
    end
{% endmacro %}
