{{ config(
    materialized='incremental',
    unique_key='sales_channel_month_key',
    incremental_strategy='merge'
) }}

with sales as (
    select
        sales_channel,
        store_sk,
        web_page_sk,
        call_center_sk,
        product_category,
        month,
        sum(quantity) as total_sales_quantity,
        sum(extended_discount_amount) as total_discount_amount,
        sum(net_paid) as total_net_paid,
        sum(net_profit) as total_net_profit,
        sum(extended_sales_price) as total_sales_amount
    from {{ ref('int_unified_sales') }}
    group by 1,2,3,4,5,6
),

returns as (
    select
        sales_channel,
        store_sk,
        web_page_sk,
        call_center_sk,
        product_category,
        month,
        sum(return_amount) as total_return_amount,
        sum(return_quantity) as total_return_quantity
    from {{ ref('int_unified_returns') }}
    --son las posiciones de como salen en el select
    group by 1,2,3,4, 5, 6
)
--hacemos un select final para juntar ventas con devoluciones
select
    s.month,
    s.sales_channel,
    --saca sales sino sacara returns
    coalesce(s.store_sk, r.store_sk) as store_sk,
    coalesce(s.web_page_sk, r.web_page_sk) as web_page_sk,
    coalesce(s.call_center_sk, r.call_center_sk) as call_center_sk,
    s.product_category,

    s.total_sales_amount,
    s.total_sales_quantity,
    s.total_discount_amount,
    s.total_net_paid,
    s.total_net_profit,

    coalesce(r.total_return_amount, 0) as total_return_amount,
    coalesce(r.total_return_quantity, 0) as total_return_quantity,

    --tasa de devolucion
    --se calcula en unidades porque es lo estandar en retail
    --nulif evita division por cero
    --coalesdce evita nulos
    coalesce(r.total_return_quantity, 0) / nullif(s.total_sales_quantity, 0) as return_rate,
    --revenue neto despues de devoluciones, lo que vendimos - lo que nos devolvieron
    s.total_sales_amount - coalesce(r.total_return_amount, 0) as net_revenue,
    --descuento medio en porcentaje, total de descuento - total de ventas
    least(
        greatest(
            s.total_discount_amount / nullif(s.total_sales_amount, 0),
            0
        ),
        1
    ) as avg_discount_pct,

    -- unique key
    --creamos una clave para que solo se actualizen las filas que cambien
    {{ dbt_utils.generate_surrogate_key([
        's.month',
        's.sales_channel',
        's.store_sk',
        's.web_page_sk',
        's.call_center_sk',
        's.product_category'
    ]) }} as sales_channel_month_key

from sales s
left join returns r
    on s.month = r.month
    and s.sales_channel = r.sales_channel
    and s.store_sk = r.store_sk
    and s.web_page_sk = r.web_page_sk
    and s.call_center_sk = r.call_center_sk
    and s.product_category = r.product_category
where s.total_sales_amount >= 0
  and s.total_sales_quantity >= 0
  and s.total_net_paid >= 0
  and s.total_net_profit >= 0
--logica jinja
--si es la primera vez que se corre el modelo, se hace un select de todo
--si no es la primera vez, se hace un select solo de los ultimos 2 meses para actualizar solo los datos recientes
--ponemos 2 meses porque puede que las devoluciones lleguen con retraso y asi nos aseguramos de que se actualicen los datos de ventas y devoluciones
{% if is_incremental() %}
  and s.month >= date_trunc('month', current_date - interval '2 months')
{% endif %}
