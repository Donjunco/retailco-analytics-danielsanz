-- Test: no puede haber ventas negativas en ningún canal

select *
from {{ ref('mart_sales_by_channel') }}
where total_sales_amount < 0
   or total_sales_quantity < 0
   or total_net_paid < 0
   or total_net_profit < 0
