with

source as (
    --seleccionamos nuestro nombre del source y la tabla de donde vamos a sacar los datos
    select * from {{ source('raw_source','CUSTOMER_ADDRESS') }}
),

--seria recomendable renombrar columnas, estandatizar tipos y limpiar datos
--pero de momento lo dejo asi
renamed as (

    select
    ca_address_sk::NUMBER as address_sk,
    ca_address_id as address_id,
    ca_street_number as street_number,
    ca_street_name as street_name,
    ca_street_type as street_type,
    ca_suite_number as suite_number,
    ca_city as city,
    ca_county as county,
    ca_state::CHAR(2) as state,
    ca_zip as zip,
    ca_country as country,
    ca_gmt_offset::NUMBER(5,2) as gmt_offset,
    ca_location_type as location_type
    from source

)

select * from renamed
