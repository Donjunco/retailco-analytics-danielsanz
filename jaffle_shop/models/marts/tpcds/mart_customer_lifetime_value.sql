{{ config(
    materialized='incremental',
    unique_key='customer_sk',
    incremental_strategy='merge'
) }}

with sales as (
    select
        bill_customer_sk as customer_sk,
        sales_channel,
        d.date as sold_date,
        net_paid,
        quantity
    from {{ ref('int_unified_sales') }} s
        left join {{ ref('stg_date_dim') }} d
        on s.sold_date_sk = d.date_sk

    {% if is_incremental() %}
    where d.date >= dateadd(month, -2, current_date)
    {% endif %}
),

returns as (
    select
        refunded_customer_sk as customer_sk,
        return_quantity
    from {{ ref('int_unified_returns') }}
)

--
select
    s.customer_sk,
    sum(s.net_paid) as clv,

    -- transacciones por canal
    count_if(s.sales_channel = 'store') as store_transactions,
    count_if(s.sales_channel = 'web') as web_transactions,
    count_if(s.sales_channel = 'catalog') as catalog_transactions,

    --primera y ultima fecha de compra
    min(s.sold_date) as first_purchase_date,
    max(s.sold_date) as last_purchase_date,
    -- tasa de devolución
    least(
        sum(coalesce(r.return_quantity, 0)) / nullif(sum(s.quantity), 0),
        1
    ) as return_rate,
    case
        when sum(s.net_paid) < {{ var('clv_p33') }} then 'low'
        when sum(s.net_paid) < {{ var('clv_p66') }} then 'medium'
        else 'high'
end as value_segment

from sales s

left join returns r
    on s.customer_sk = r.customer_sk
where s.customer_sk is not null
    group by 1

