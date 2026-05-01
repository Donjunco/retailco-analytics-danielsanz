{% macro classify_trend(metric, partition_by, order_by) %}
    case
        --comparamos el valor actual de la métrica con el valor de la métrica en la fila anterior, ordenada por la columna especificada. Si el valor actual es mayor, la tendencia es 'creciente', si es menor, la tendencia es 'decreciente', y si es igual, la tendencia es 'estable'.
        when {{ metric }} > lag({{ metric }}) over (partition by {{ partition_by }} order by {{ order_by }})
            then 'creciente'
        when {{ metric }} < lag({{ metric }}) over (partition by {{ partition_by }} order by {{ order_by }})
            then 'decreciente'
        else 'estable'
    end
{% endmacro %}
