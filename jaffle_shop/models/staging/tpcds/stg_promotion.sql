with

source as (
    --seleccionamos nuestro nombre del source y la tabla de donde vamos a sacar los datos
    select * from {{ source('raw_source','PROMOTION') }}
),

--seria recomendable renombrar columnas, estandatizar tipos y limpiar datos
--pero de momento lo dejo asi
renamed as (

    select
    p_promo_sk::NUMBER as promotion_sk,
    p_promo_id as promotion_id,
    p_start_date_sk as start_date_sk,
    p_end_date_sk as end_date_sk,
    p_item_sk::NUMBER as item_sk,
    p_cost as cost,
    p_response_target as response_target,
    p_promo_name as promotion_name,
    (coalesce(p_channel_dmail, 'N') = 'Y') as channel_direct_mail,
    (coalesce(p_channel_email, 'N') = 'Y') as channel_email,
    (coalesce(p_channel_catalog, 'N') = 'Y') as channel_catalog,
    (coalesce(p_channel_tv, 'N') = 'Y') as channel_tv,
    (coalesce(p_channel_radio, 'N') = 'Y') as channel_radio,
    (coalesce(p_channel_press, 'N') = 'Y') as channel_press,
    (coalesce(p_channel_event, 'N') = 'Y') as channel_event,
    (coalesce(p_channel_demo, 'N') = 'Y') as channel_demo,
    (coalesce(p_channel_details, 'N') = 'Y') as channel_details,
    p_purpose as purpose,
    (coalesce(p_discount_active, 'N') = 'Y') as discount_active
    from source

)

select * from renamed
