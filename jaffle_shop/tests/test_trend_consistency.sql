-- Test: si el stock sube respecto a la semana anterior, la tendencia no puede ser 'decreciente'

with data as (
    select
        item_sk,
        warehouse_sk,
        week,
        avg_stock_week,
        trend,
        lag(avg_stock_week) over (
            partition by item_sk, warehouse_sk
            order by week
        ) as prev_stock
    from {{ ref('mart_inventory_health') }}
)

select *
from data
where prev_stock is not null
  and avg_stock_week > prev_stock
  and trend = 'decreciente'
