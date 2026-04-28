with

source as (
    --seleccionamos nuestro nombre del source y la tabla de donde vamos a sacar los datos
    select * from {{ source('raw_source','INVENTORY') }}
),

--seria recomendable renombrar columnas, estandatizar tipos y limpiar datos
--pero de momento lo dejo asi
renamed as (

    select
    inv_date_sk::NUMBER as date_sk,
    inv_item_sk::NUMBER as item_sk,
    inv_warehouse_sk::NUMBER as warehouse_sk,
    inv_quantity_on_hand as quantity_on_hand
    from source

)

select * from renamed
