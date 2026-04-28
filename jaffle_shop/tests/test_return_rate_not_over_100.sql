-- Test: la tasa de devolución no puede superar el 100%

select *
from {{ ref('mart_customer_lifetime_value') }}
where return_rate > 1
