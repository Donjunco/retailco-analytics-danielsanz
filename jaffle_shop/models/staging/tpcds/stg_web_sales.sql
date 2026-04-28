with

source as (
    --seleccionamos nuestro nombre del source y la tabla de donde vamos a sacar los datos
    select * from {{ source('raw_source','WEB_SALES') }}
),

--seria recomendable renombrar columnas, estandatizar tipos y limpiar datos
--pero de momento lo dejo asi
renamed as (

    select
    ws_sold_date_sk::NUMBER as sold_date_sk,
    ws_sold_time_sk::NUMBER as sold_time_sk,
    ws_ship_date_sk::NUMBER as ship_date_sk,
    ws_item_sk::NUMBER as item_sk,
    ws_bill_customer_sk::NUMBER as bill_customer_sk,
    ws_bill_cdemo_sk::NUMBER as bill_customer_demographics_sk,
    ws_bill_hdemo_sk::NUMBER as bill_household_demographics_sk,
    ws_bill_addr_sk::NUMBER as bill_address_sk,
    ws_ship_customer_sk::NUMBER as ship_customer_sk,
    ws_ship_cdemo_sk::NUMBER as ship_customer_demographics_sk,
    ws_ship_hdemo_sk::NUMBER as ship_household_demographics_sk,
    ws_ship_addr_sk::NUMBER as ship_address_sk,
    ws_web_page_sk::NUMBER as web_page_sk,
    ws_web_site_sk::NUMBER as web_site_sk,
    ws_ship_mode_sk::NUMBER as ship_mode_sk,
    ws_warehouse_sk::NUMBER as warehouse_sk,
    ws_promo_sk::NUMBER as promotion_sk,
    ws_order_number as order_number,
    ws_quantity as quantity,
    ws_wholesale_cost::NUMBER(7,2) as wholesale_cost,
    ws_list_price::NUMBER(7,2) as list_price,
    ws_sales_price::NUMBER(7,2) as sales_price,
    ws_ext_discount_amt::NUMBER(7,2) as extended_discount_amount,
    ws_ext_sales_price::NUMBER(7,2) as extended_sales_price,
    ws_ext_wholesale_cost::NUMBER(7,2) as extended_wholesale_cost,
    ws_ext_list_price::NUMBER(7,2) as extended_list_price,
    ws_ext_tax::NUMBER(7,2) as extended_tax,
    ws_coupon_amt::NUMBER(7,2) as coupon_amount,
    ws_ext_ship_cost::NUMBER(7,2) as extended_shipping_cost,
    ws_net_paid::NUMBER(7,2) as net_paid,
    ws_net_paid_inc_tax::NUMBER(7,2) as net_paid_including_tax,
    ws_net_paid_inc_ship::NUMBER(7,2) as net_paid_including_ship,
    ws_net_paid_inc_ship_tax::NUMBER(7,2) as net_paid_including_ship_and_tax,
    ws_net_profit::NUMBER(7,2) as net_profit
    from source

)

select * from renamed
