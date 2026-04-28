with

source as (
    --seleccionamos nuestro nombre del source y la tabla de donde vamos a sacar los datos
    select * from {{ source('raw_source','DATE_DIM') }}
),

--seria recomendable renombrar columnas, estandatizar tipos y limpiar datos
renamed as (

    select
    d_date_sk::NUMBER as date_sk,
    d_date_id as date_id,
    try_to_date(d_date) as date,
    d_month_seq as month_seq,
    d_week_seq as week_seq,
    d_quarter_seq as quarter_seq,
    d_year as year,
    d_dow as day_of_week,
    d_moy as month_of_year,
    d_dom as day_of_month,
    d_qoy as quarter_of_year,
    d_fy_year as fiscal_year,
    d_fy_quarter_seq as fiscal_quarter_seq,
    d_fy_week_seq as fiscal_week_seq,
    d_day_name as day_name,
    d_quarter_name as quarter_name,
    --para que salga true en vez de y o n
    (coalesce(d_holiday, 'N') = 'Y') as is_holiday,
    (coalesce(d_weekend, 'N') = 'Y') as is_weekend,
    (coalesce(d_following_holiday, 'N') = 'Y') as is_following_holiday,
    d_first_dom as first_day_of_month,
    d_last_dom as last_day_of_month,
    d_same_day_ly as same_day_last_year,
    d_same_day_lq as same_day_last_quarter,
    (coalesce(d_current_day, 'N') = 'Y') as is_current_day,
    (coalesce(d_current_week, 'N') = 'Y') as is_current_week,
    (coalesce(d_current_month, 'N') = 'Y') as is_current_month,
    (coalesce(d_current_quarter, 'N') = 'Y') as is_current_quarter,
    (coalesce(d_current_year, 'N') = 'Y') as is_current_year
    from source

)

select * from renamed
