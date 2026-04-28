with

source as (
    --seleccionamos nuestro nombre del source y la tabla de donde vamos a sacar los datos
    select * from {{ source('raw_source','CATALOG_RETURNS') }}
),

--seria recomendable renombrar columnas, estandatizar tipos y limpiar datos
--pero de momento lo dejo asi
renamed as (

    select 
    cr_returned_date_sk::NUMBER as returned_date_sk,
    cr_returned_time_sk::NUMBER as returned_time_sk,
    cr_item_sk::NUMBER as item_sk,
    cr_refunded_customer_sk::NUMBER as refunded_customer_sk,
    cr_refunded_cdemo_sk::NUMBER as refunded_customer_demographics_sk,
    cr_refunded_hdemo_sk::NUMBER as refunded_household_demographics_sk,
    cr_refunded_addr_sk::NUMBER as refunded_address_sk,
    cr_returning_customer_sk::NUMBER as returning_customer_sk,
    cr_returning_cdemo_sk::NUMBER as returning_customer_demographics_sk,
    cr_returning_hdemo_sk::NUMBER as returning_household_demographics_sk,
    cr_returning_addr_sk::NUMBER as returning_address_sk,
    cr_call_center_sk::NUMBER as call_center_sk,
    cr_catalog_page_sk::NUMBER as catalog_page_sk,
    cr_ship_mode_sk::NUMBER as ship_mode_sk,
    cr_warehouse_sk::NUMBER as warehouse_sk,
    cr_reason_sk::NUMBER as reason_sk,
    cr_order_number as order_number,
    cr_return_quantity as return_quantity,
    cr_return_amount::NUMBER(7,2) as return_amount,
    cr_return_tax::NUMBER(7,2) as return_tax,
    cr_return_amt_inc_tax::NUMBER(7,2) as return_amount_including_tax,
    cr_fee::NUMBER(7,2) as fee,
    cr_return_ship_cost::NUMBER(7,2) as return_ship_cost,
    cr_refunded_cash::NUMBER(7,2) as refunded_cash,
    cr_reversed_charge::NUMBER(7,2) as reversed_charge,
    cr_store_credit::NUMBER(7,2) as store_credit,
    cr_net_loss::NUMBER(7,2) as net_loss
    from source

)

select * from renamed
