-- Test: no puede haber stock medio semanal negativo

select *
from {{ ref('mart_inventory_health') }}
where avg_stock_week < 0
