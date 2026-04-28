with

source as (
    --seleccionamos nuestro nombre del source y la tabla de donde vamos a sacar los datos
    select * from {{ source('raw_source','ITEM') }}
),

--seria recomendable renombrar columnas, estandatizar tipos y limpiar datos
--pero de momento lo dejo asi
renamed as (

    select 
    i_item_sk::NUMBER as item_sk,
    i_item_id as item_id,
    try_to_date(i_rec_start_date) as record_start_date,
    try_to_date(i_rec_end_date) as record_end_date,
    i_item_desc as item_desc,
    i_current_price::NUMBER(7,2) as current_price,
    i_wholesale_cost::NUMBER(7,2) as wholesale_cost,
    i_brand_id as brand_id,
    i_brand as brand,
    i_class_id as class_id,
    i_class as class,
    i_category_id as category_id,
    i_category as category,
    i_manufact_id as manufacturer_id,
    i_manufact as manufacturer,
    i_size as size,
    i_formulation as formulation,
    i_color as color,
    i_units as units,
    i_container as container,
    i_manager_id as manager_id,
    i_product_name as product_name
    from source

)

select * from renamed
