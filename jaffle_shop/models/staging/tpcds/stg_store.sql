with

source as (
    --seleccionamos nuestro nombre del source y la tabla de donde vamos a sacar los datos
    select * from {{ source('raw_source','STORE') }}
),

--seria recomendable renombrar columnas, estandatizar tipos y limpiar datos
--pero de momento lo dejo asi
renamed as (

    select
    s_store_sk::NUMBER as store_sk,
    s_store_id as store_id,
    try_to_date(s_rec_start_date) as record_start_date,
    try_to_date(s_rec_end_date) as record_end_date,
    s_closed_date_sk as closed_date,
    s_store_name as store_name,
    s_number_employees as number_employees,
    s_floor_space as floor_space,
    s_hours as hours,
    s_manager as manager,
    s_market_id as market_id,
    s_geography_class as geography_class,
    s_market_desc as market_description,
    s_market_manager as market_manager,
    s_division_id as division_id,
    s_division_name as division_name,
    s_company_id as company_id,
    s_company_name as company_name,
    s_street_number as street_number,
    s_street_name as street_name,
    s_street_type as street_type,
    s_suite_number as suite_number,
    s_city as city,
    s_county as county,
    s_state::CHAR(2) as state,
    s_zip as zip,
    s_country as country,
    s_gmt_offset as gmt_offset,
    from source

)

select * from renamed
