{{ config(
    materialized='incremental',
    unique_key=['item_sk', 'warehouse_sk', 'week'],
    incremental_strategy='merge'
) }}

with inventory as (
    select
    date_sk,
    item_sk,
    warehouse_sk,
    quantity_on_hand
    from {{ ref('stg_inventory') }}

),

sales as (
    select
    sold_date_sk as date_sk,
    item_sk,
    quantity
    from {{ ref('int_unified_sales') }}

),
--metrica 1 stock medio semanal
inventory_weekly as (
    select
        item_sk,
        warehouse_sk,
        weekofyear(d.date) as week,
        avg(quantity_on_hand) as avg_stock_week
    from inventory i
    join {{ ref('stg_date_dim') }} d
        on i.date_sk = d.date_sk
    group by 1,2,3
),
-- ventas medias semanales
sales_weekly as (
    select
        item_sk,
        weekofyear(d.date) as week,
        avg(quantity) as avg_sales_week
    from sales s
    join {{ ref('stg_date_dim') }} d
        on s.date_sk = d.date_sk
    group by 1,2
),
combined as (
    select
        iw.item_sk,
        iw.warehouse_sk,
        iw.week,
        iw.avg_stock_week,
        sw.avg_sales_week,
        --semanas de stock esta en una macro
        {{ calc_weeks_of_stock('iw.avg_stock_week', 'sw.avg_sales_week') }} as weeks_of_stock,
        --semanas con rotura de stock
        case when iw.avg_stock_week = 0 then 1 else 0 end as stockout_flag,
        --clasificacion tendecia esta en una macron
        {{ classify_trend('iw.avg_stock_week', 'iw.item_sk', 'iw.week') }} as trend
    from inventory_weekly iw
    left join sales_weekly sw
        on iw.item_sk = sw.item_sk
       and iw.week = sw.week
)

select * from combined

