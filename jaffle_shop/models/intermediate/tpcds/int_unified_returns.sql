with store_returns as (
select
    'store' as sales_channel,
    s.return_date_sk as returned_date_sk,
    s.return_time_sk as returned_time_sk,
    s.item_sk,
    s.customer_sk as refunded_customer_sk,
    s.customer_demographics_sk as refunded_customer_demographics_sk,
    s.household_demographics_sk as refunded_household_demographics_sk,
    s.address_sk as refunded_address_sk,
    null as returning_customer_sk,
    null as returning_customer_demographics_sk,
    null as returning_household_demographics_sk,
    null as returning_address_sk,
    s.ticket_number as order_number,
    s.return_quantity,
    s.return_amount,
    s.return_tax,
    s.return_amount_including_tax,
    s.fee,
    s.return_ship_cost,
    s.refunded_cash,
    s.reversed_charge,
    s.store_credit,
    s.net_loss,
    s.store_sk,
    null as call_center_sk,
    null as web_page_sk,

    i.category as product_category,
    date_trunc('month', d.date) as month

    from {{ ref('stg_store_returns') }} s 
    left join {{ ref('stg_item') }} i on s.item_sk = i.item_sk
    left join {{ ref('stg_date_dim') }} d on s.return_date_sk = d.date_sk

),

catalog_returns as (
    select
    'catalog' as sales_channel,
    s.returned_date_sk,
    s.returned_time_sk,
    s.item_sk,
    s.refunded_customer_sk,
    s.refunded_customer_demographics_sk,
    s.refunded_household_demographics_sk,
    s.refunded_address_sk,
    s.returning_customer_sk,
    s.returning_customer_demographics_sk,
    s.returning_household_demographics_sk,
    s.returning_address_sk,
    s.order_number,
    s.return_quantity,
    s.return_amount,
    s.return_tax,
    s.return_amount_including_tax,
    s.fee,
    s.return_ship_cost,
    s.refunded_cash,
    s.reversed_charge,
    s.store_credit,
    s.net_loss,
    null as store_sk,
    s.call_center_sk,
    null as web_site_sk,
    
    i.category as product_category,
    date_trunc('month', d.date) as month

    from {{ ref('stg_catalog_returns') }} s
    left join {{ ref('stg_item') }} i on s.item_sk = i.item_sk
    left join {{ ref('stg_date_dim') }} d on s.returned_date_sk = d.date_sk
),

web_returns as (
    select
    'web' as sales_channel,
    s.returned_date_sk,
    s.returned_time_sk,
    s.item_sk,
    s.refunded_customer_sk,
    s.refunded_customer_demographics_sk,
    s.refunded_household_demographics_sk,
    s.refunded_address_sk,
    s.returning_customer_sk,
    s.returning_customer_demographics_sk,
    s.returning_household_demographics_sk,
    s.returning_address_sk,
    s.order_number,
    s.return_quantity,
    s.return_amount,
    s.return_tax,
    s.return_amount_including_tax,
    s.fee,
    s.return_ship_cost,
    s.refunded_cash,
    s.reversed_charge,
    s.account_credit,
    s.net_loss,
    null as store_sk,
    null as call_center_sk,
    s.web_page_sk,

    i.category as product_category,
    date_trunc('month', d.date) as month

    from {{ ref('stg_web_returns') }} s 
    left join {{ ref('stg_item') }} i on s.item_sk = i.item_sk
    left join {{ ref('stg_date_dim') }} d on s.returned_date_sk = d.date_sk
)

select * from store_returns
union all
select * from catalog_returns
union all
select * from web_returns
