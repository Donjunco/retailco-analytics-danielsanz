with

source as (
    --seleccionamos nuestro nombre del source y la tabla de donde vamos a sacar los datos
    select * from {{ source('raw_source','CATALOG_SALES') }}
),

--seria recomendable renombrar columnas, estandatizar tipos y limpiar datos
--pero de momento lo dejo asi
renamed as (

    select 
    cs_sold_date_sk::NUMBER as sold_date_sk,
    cs_sold_time_sk::NUMBER as sold_time_sk,
    cs_ship_date_sk::NUMBER as ship_date_sk,
    cs_bill_customer_sk::NUMBER as bill_customer_sk,
    cs_bill_cdemo_sk::NUMBER as bill_customer_demographics_sk,
    cs_bill_hdemo_sk::NUMBER as bill_household_demographics_sk,
    cs_bill_addr_sk::NUMBER as bill_address_sk,
    cs_ship_customer_sk::NUMBER as ship_customer_sk,
    cs_ship_cdemo_sk::NUMBER as ship_customer_demographics_sk,
    cs_ship_hdemo_sk::NUMBER as ship_household_demographics_sk,
    cs_ship_addr_sk::NUMBER as ship_address_sk,
    cs_call_center_sk::NUMBER as call_center_sk,
    cs_catalog_page_sk::NUMBER as catalog_page_sk,
    cs_ship_mode_sk::NUMBER as ship_mode_sk,
    cs_warehouse_sk::NUMBER as warehouse_sk,
    cs_item_sk::NUMBER as item_sk,
    cs_promo_sk::NUMBER as promotion_sk,
    cs_order_number as order_number,
    cs_quantity as quantity,
    cs_wholesale_cost::NUMBER(7,2) as wholesale_cost,
    cs_list_price::NUMBER(7,2) as list_price,
    cs_sales_price::NUMBER(7,2) as sales_price,
    cs_ext_discount_amt::NUMBER(7,2) as extended_discount_amount,
    cs_ext_sales_price::NUMBER(7,2) as extended_sales_price,
    cs_ext_wholesale_cost::NUMBER(7,2) as extended_wholesale_cost,
    cs_ext_list_price::NUMBER(7,2) as extended_list_price,
    cs_ext_tax::NUMBER(7,2) as extended_tax,
    cs_coupon_amt::NUMBER(7,2) as coupon_amount,
    cs_ext_ship_cost::NUMBER(7,2) as extended_ship_cost,
    cs_net_paid::NUMBER(7,2) as net_paid,
    cs_net_paid_inc_tax::NUMBER(7,2) as net_paid_including_tax,
    cs_net_paid_inc_ship::NUMBER(7,2) as net_paid_including_ship,
    cs_net_paid_inc_ship_tax::NUMBER(7,2) as net_paid_including_ship_tax,
    cs_net_profit::NUMBER(7,2) as net_profit
    from source

)

select * from renamed
