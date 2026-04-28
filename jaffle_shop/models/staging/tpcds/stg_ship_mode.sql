with

source as (
    --seleccionamos nuestro nombre del source y la tabla de donde vamos a sacar los datos
    select * from {{ source('raw_source','SHIP_MODE') }}
),

--seria recomendable renombrar columnas, estandatizar tipos y limpiar datos
--pero de momento lo dejo asi
renamed as (

    select
    sm_ship_mode_sk::NUMBER as ship_mode_sk,
    sm_ship_mode_id as ship_mode_id,
    sm_type as type,
    sm_code as code,
    sm_carrier as carrier,
    sm_contract as contract 
    from source

)

select * from renamed
