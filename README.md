## descripción del proyecto:

Este proyecto construye un Data Warehouse analítico utilizando dbt Core sobre un subset de TPC‑DS ya cargado en el esquema RAW de Snowflake.
El objetivo es transformar estos datos en un modelo dimensional (staging → intermediate → marts) que permita analizar el rendimiento de un retailer multicanal y consumir los resultados desde un dashboard en Power BI.

## instrucciones para reproducirlo,

Clonar el repositorio.

Configurar profiles.yml para conectar dbt con Snowflake.

Instalar dependencias:
dbt deps

Crear modelos y tests.

Ejecutar el pipeline:
dbt run

Ejecutar tests:
dbt test

Para ejecutar el dbt docs:
dbtf compile --write-catalog y activar y meterme con la extension live server.

Conectar Power BI a Snowflake y cargar las tablas del esquema MARTS.

## lista de modelos, 

-Staging (stg_)
Modelos que limpian y estandarizan datos RAW:

stg_catalog_returns
stg_catalog_sales
stg_customer_address
stg_customer_demographics
stg_customer
stg_date_dim
stg_household_demographics
stg_inventory
stg_item
stg_promotion
stg_reason
stg_ship_mode
stg_store_returns
stg_store_sales
stg_store
stg_web_returns
stg_web_sales

todos con sus respectivos .yml

-Intermediate (int_)
Modelos con lógica de negocio:

int_unified_returns
int_customer_enriched
int_unified_sales


-Marts (dim_ y fct_)
Modelos dimensionales para análisis:

mart_customer_lifetime_value
mart_inventory_health
mart_sales_by_channel

todos con sus respectivos .yml

## decisiones de diseño principales.

Uso de arquitectura en 3 capas para claridad y mantenibilidad.
Construcción de un modelo estrella con hechos granulares y dimensiones conformadas.
Unificación de ventas de tienda física y web en un único hecho analítico.
Implementación de tests de calidad con dbt nativo y dbt_expectations.
Power BI conectado directamente a los MARTS para análisis consistente y eficiente.