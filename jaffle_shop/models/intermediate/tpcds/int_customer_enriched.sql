    --- Esta vista enriquecida del cliente combina información de varias tablas relacionadas con el cliente, como su dirección, demografía y datos del hogar. Esto permite un análisis más completo del cliente y su comportamiento de compra.
    with customer as(
        select * from {{ ref('stg_customer') }}
    ),
    address as(
        select * from {{ ref('stg_customer_address') }}
    ),
    customer_demographics as(
        select * from {{ ref('stg_customer_demographics') }}
    ),
    household_demographics as(
        select * from {{ ref('stg_household_demographics') }}
    )

    select
    customer.customer_sk,
    customer.customer_id,
    customer.salutation,
    customer.first_name,
    customer.last_name,
    customer.birth_day,
    customer.birth_month,
    customer.birth_year,
    customer.birth_country,
    customer.login,
    customer.email_address,
    customer.last_review_date,

    address.address_sk,
    address.street_number,
    address.city,
    address.county,
    address.state,

    customer_demographics.education_status,
    customer_demographics.marital_status,
    customer_demographics.gender,
    customer_demographics.purchase_estimate,
    customer_demographics.credit_rating,

    household_demographics.income_band_sk,
    household_demographics.buy_potential,
    household_demographics.dep_count,
    household_demographics.vehicle_count



    from customer
    left join address on customer.current_address_sk = address.address_sk
    left join customer_demographics on customer.current_customer_demographics_sk = customer_demographics.demo_sk
    left join household_demographics on customer.current_household_demographics_sk = household_demographics.demo_sk