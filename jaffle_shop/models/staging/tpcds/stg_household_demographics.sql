with

source as (
    --seleccionamos nuestro nombre del source y la tabla de donde vamos a sacar los datos
    select * from {{ source('raw_source','HOUSEHOLD_DEMOGRAPHICS') }}
),

--seria recomendable renombrar columnas, estandatizar tipos y limpiar datos
--pero de momento lo dejo asi
renamed as (

    select
    hd_demo_sk::NUMBER as demo_sk,
    hd_income_band_sk::NUMBER as income_band_sk,
    hd_buy_potential as buy_potential,
    hd_dep_count as dep_count,
    hd_vehicle_count as vehicle_count
    from source

)

select * from renamed
