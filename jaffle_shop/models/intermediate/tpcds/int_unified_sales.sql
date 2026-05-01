--Esta vista unificada combina las ventas de tienda, catálogo y web en una sola tabla, con una columna adicional que indica el canal de venta. Esto facilita el análisis de las ventas por canal y permite comparar el rendimiento entre ellos.
with store_sales as (
select
    'store' as sales_channel,
    s.sold_date_sk,
    s.sold_time_sk,
    s.item_sk,
    s.customer_sk as bill_customer_sk,
    s.customer_demographics_sk as bill_customer_demographics_sk,
    s.household_demographics_sk as bill_household_demographics_sk,
    s.address_sk as bill_address_sk,
    null as ship_customer_sk,
    null as ship_customer_demographics_sk,
    null as ship_household_demographics_sk,
    null as ship_address_sk,
    s.promotion_sk,
    s.ticket_number as order_number,
    s.quantity,
    s.wholesale_cost,
    s.list_price,
    s.sales_price,
    s.extended_discount_amount,
    s.extended_sales_price,
    s.extended_wholesale_cost,
    s.extended_list_price,
    s.extended_tax,
    s.coupon_amount,
    s.net_paid,
    s.net_profit,
    s.store_sk,
    null as call_center_sk,
    null as web_page_sk,

    i.category as product_category,
    --agrupamos por mes para facilitar el análisis temporal, se puede cambiar a semana o trimestre si se prefiere
    date_trunc('month', d.date) as month



    from {{ ref('stg_store_sales') }} s
    left join {{ ref('stg_item') }} i on s.item_sk = i.item_sk
    left join {{ ref('stg_date_dim') }} d on s.sold_date_sk = d.date_sk

),

catalog_sales as (
    select
    'catalog' as sales_channel,
    s.sold_date_sk,
    s.sold_time_sk,
    s.item_sk,
    s.bill_customer_sk,
    s.bill_customer_demographics_sk,
    s.bill_household_demographics_sk,
    s.bill_address_sk,
    s.ship_customer_sk,
    s.ship_customer_demographics_sk,
    s.ship_household_demographics_sk,
    s.ship_address_sk,
    s.promotion_sk,
    s.order_number,
    s.quantity,
    s.wholesale_cost,
    s.list_price,
    s.sales_price,
    s.extended_discount_amount,
    s.extended_sales_price,
    s.extended_wholesale_cost,
    s.extended_list_price,
    s.extended_tax,
    s.coupon_amount,
    s.net_paid,
    s.net_profit,
    null as store_sk,
    s.call_center_sk,
    null as web_page_sk,

    i.category as product_category,
    date_trunc('month', d.date) as month

    from {{ ref('stg_catalog_sales') }} s
    left join {{ ref('stg_item') }} i on s.item_sk = i.item_sk
    left join {{ ref('stg_date_dim') }} d on s.sold_date_sk = d.date_sk
),

web_sales as (
    select
    'web' as sales_channel,
    s.sold_date_sk,
    s.sold_time_sk,
    s.item_sk,
    s.bill_customer_sk,
    s.bill_customer_demographics_sk,
    s.bill_household_demographics_sk,
    s.bill_address_sk,
    s.ship_customer_sk,
    s.ship_customer_demographics_sk,
    s.ship_household_demographics_sk,
    s.ship_address_sk,
    s.promotion_sk,
    s.order_number,
    s.quantity,
    s.wholesale_cost,
    s.list_price,
    s.sales_price,
    s.extended_discount_amount,
    s.extended_sales_price,
    s.extended_wholesale_cost,
    s.extended_list_price,
    s.extended_tax,
    s.coupon_amount,
    s.net_paid,
    s.net_profit,
    null as store_sk,
    null as call_center_sk,
    s.web_page_sk,

    i.category as product_category,
    date_trunc('month', d.date) as month

    from {{ ref('stg_web_sales') }} s
    left join {{ ref('stg_item') }} i on s.item_sk = i.item_sk
    left join {{ ref('stg_date_dim') }} d on s.sold_date_sk = d.date_sk
)

select * from store_sales
union all
select * from catalog_sales
union all
select * from web_sales
