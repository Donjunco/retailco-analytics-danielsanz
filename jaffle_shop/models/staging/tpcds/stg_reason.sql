with

source as (
    --seleccionamos nuestro nombre del source y la tabla de donde vamos a sacar los datos
    select * from {{ source('raw_source','REASON') }}
),

--seria recomendable renombrar columnas, estandatizar tipos y limpiar datos
--pero de momento lo dejo asi
renamed as (

    select
    r_reason_sk::NUMBER as reason_sk,
    r_reason_id as reason_id,
    r_reason_desc as reason_description
    from source

)

select * from renamed
