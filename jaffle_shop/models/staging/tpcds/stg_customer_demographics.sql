with

source as (
    --seleccionamos nuestro nombre del source y la tabla de donde vamos a sacar los datos
    select * from {{ source('raw_source','CUSTOMER_DEMOGRAPHICS') }}
),

--seria recomendable renombrar columnas, estandatizar tipos y limpiar datos
--pero de momento lo dejo asi
renamed as (

    select
    cd_demo_sk::NUMBER as demo_sk,
    cd_gender as gender,
    cd_marital_status as marital_status,
    cd_education_status as education_status,
    cd_purchase_estimate as purchase_estimate,
    cd_credit_rating as credit_rating,
    cd_dep_count as dep_count,
    cd_dep_employed_count as dep_employed_count,
    cd_dep_college_count as dep_college_count
    from source

)

select * from renamed
