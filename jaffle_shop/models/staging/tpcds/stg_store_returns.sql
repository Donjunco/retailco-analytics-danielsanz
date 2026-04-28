with

source as (
    --seleccionamos nuestro nombre del source y la tabla de donde vamos a sacar los datos
    select * from {{ source('raw_source','STORE_RETURNS') }}
),

--seria recomendable renombrar columnas, estandatizar tipos y limpiar datos
--pero de momento lo dejo asi
renamed as (

    select
    sr_returned_date_sk::NUMBER as return_date_sk,
    sr_return_time_sk::NUMBER as return_time_sk,
    sr_item_sk::NUMBER as item_sk,
    sr_customer_sk::NUMBER as customer_sk,
    sr_cdemo_sk::NUMBER as customer_demographics_sk,
    sr_hdemo_sk::NUMBER as household_demographics_sk,
    sr_addr_sk::NUMBER as address_sk,
    sr_store_sk::NUMBER as store_sk,
    sr_reason_sk::NUMBER as reason_sk,
    sr_ticket_number as ticket_number,
    sr_return_quantity as return_quantity,
    sr_return_amt::NUMBER(7,2) as return_amount,
    sr_return_tax::NUMBER(7,2) as return_tax,
    sr_return_amt_inc_tax::NUMBER(7,2) as return_amount_including_tax,
    sr_fee::NUMBER(7,2) as fee,
    sr_return_ship_cost::NUMBER(7,2) as return_ship_cost,
    sr_refunded_cash::NUMBER(7,2) as refunded_cash,
    sr_reversed_charge::NUMBER(7,2) as reversed_charge,
    sr_store_credit::NUMBER(7,2) as store_credit,
    sr_net_loss::NUMBER(7,2) as net_loss
    from source

)

select * from renamed
