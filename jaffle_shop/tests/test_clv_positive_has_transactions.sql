-- Test: ningún cliente con CLV positivo puede tener 0 transacciones

with data as (
    select
        customer_sk,
        clv,
        store_transactions,
        web_transactions,
        catalog_transactions
    from {{ ref('mart_customer_lifetime_value') }}
)

select *
from data
--filtro para quedarnos solo con clientes con CLV positivo y sin transacciones
where clv > 0
  and (coalesce(store_transactions, 0)
     + coalesce(web_transactions, 0)
     + coalesce(catalog_transactions, 0)) = 0
