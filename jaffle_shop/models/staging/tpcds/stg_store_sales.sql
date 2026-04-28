with

source as (
    --seleccionamos nuestro nombre del source y la tabla de donde vamos a sacar los datos
    select * from {{ source('raw_source','STORE_SALES') }}
),

--seria recomendable renombrar columnas, estandatizar tipos y limpiar datos
--pero de momento lo dejo asi
renamed as (

    select
    ss_sold_date_sk::NUMBER as sold_date_sk,
    ss_sold_time_sk::NUMBER as sold_time_sk,
    ss_item_sk::NUMBER as item_sk,
    ss_customer_sk::NUMBER as customer_sk,
    ss_cdemo_sk::NUMBER as customer_demographics_sk,
    ss_hdemo_sk::NUMBER as household_demographics_sk,
    ss_addr_sk::NUMBER as address_sk,
    ss_store_sk::NUMBER as store_sk,
    ss_promo_sk::NUMBER as promotion_sk,
    ss_ticket_number as ticket_number,
    ss_quantity as quantity,
    ss_wholesale_cost::NUMBER(7,2) as wholesale_cost,
    ss_list_price::NUMBER(7,2) as list_price,
    ss_sales_price::NUMBER(7,2) as sales_price,
    ss_ext_discount_amt::NUMBER(7,2) as extended_discount_amount,
    ss_ext_sales_price::NUMBER(7,2) as extended_sales_price,
    ss_ext_wholesale_cost::NUMBER(7,2) as extended_wholesale_cost,
    ss_ext_list_price::NUMBER(7,2) as extended_list_price,
    ss_ext_tax::NUMBER(7,2) as extended_tax,
    ss_coupon_amt::NUMBER(7,2) as coupon_amount,
    ss_net_paid::NUMBER(7,2) as net_paid,
    ss_net_paid_inc_tax::NUMBER(7,2) as net_paid_inc_tax,
    ss_net_profit::NUMBER(7,2) as net_profit
    from source

)

select * from renamed
