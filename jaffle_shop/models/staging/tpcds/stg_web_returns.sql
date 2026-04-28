with

source as (
    --seleccionamos nuestro nombre del source y la tabla de donde vamos a sacar los datos
    select * from {{ source('raw_source','WEB_RETURNS') }}
),

--seria recomendable renombrar columnas, estandatizar tipos y limpiar datos
--pero de momento lo dejo asi
renamed as (

    select
    wr_returned_date_sk::NUMBER as returned_date_sk,
    wr_returned_time_sk::NUMBER as returned_time_sk,
    wr_item_sk::NUMBER as item_sk,
    wr_refunded_customer_sk::NUMBER as refunded_customer_sk,
    wr_refunded_cdemo_sk::NUMBER as refunded_customer_demographics_sk,
    wr_refunded_hdemo_sk::NUMBER as refunded_household_demographics_sk,
    wr_refunded_addr_sk::NUMBER as refunded_address_sk,
    wr_returning_customer_sk::NUMBER as returning_customer_sk,
    wr_returning_cdemo_sk::NUMBER as returning_customer_demographics_sk,
    wr_returning_hdemo_sk::NUMBER as returning_household_demographics_sk,
    wr_returning_addr_sk::NUMBER as returning_address_sk,
    wr_web_page_sk::NUMBER as web_page_sk,
    wr_reason_sk::NUMBER as reason_sk,
    wr_order_number as order_number,
    wr_return_quantity as return_quantity,
    wr_return_amt::NUMBER(7,2) as return_amount,
    wr_return_tax::NUMBER(7,2) as return_tax,
    wr_return_amt_inc_tax::NUMBER(7,2) as return_amount_including_tax,
    wr_fee::NUMBER(7,2) as fee,
    wr_return_ship_cost::NUMBER(7,2) as return_ship_cost,
    wr_refunded_cash::NUMBER(7,2) as refunded_cash,
    wr_reversed_charge::NUMBER(7,2) as reversed_charge,
    wr_account_credit::NUMBER(7,2) as account_credit,
    wr_net_loss::NUMBER(7,2) as net_loss
    from source

)

select * from renamed
