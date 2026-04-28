-- Test: net_revenue debe ser igual a total_sales_amount - total_return_amount

select *
from {{ ref('mart_sales_by_channel') }}
where net_revenue != total_sales_amount - total_return_amount
