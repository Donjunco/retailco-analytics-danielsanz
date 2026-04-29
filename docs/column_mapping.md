
Este documento describe las decisiones tomadas para unificar y estandarizar
las tablas de ventas, devoluciones y clientes dentro del proyecto. Que son los archivos de intermediate.

---

## 1. int_unified_sales

### OBJETIVO
Las tablas `store_sales`, `catalog_sales` y `web_sales` se han unificado en
el modelo `int_unified_sales`. Para tener todo el tema de ventas en una unica tabla.

### Decisiones de mapeo
- `ticket_number` (store) → `order_number`, lo cambiamos para tener todos lo mismo
- `web_page_sk` se utiliza como clave del canal web, ya que es la columna
  no esta disponible en los returns para que este igual esa parte tanto
  sales como returns.
-`customer`: aqui lo que hicimos fue cambiar en caso del store nuestro customer a bill y añadir como null los clientes de ship.
-añadimos la sk dependiendo si es store, web o page.
- añadimos i.category para sacar la categoria del producto en el marts y añadimos el mes tambien.

### Reglas de unificación
- Tendremos el mismo orden de los campos en los 3 modelos.
-añadiremos nulls en caso de que alguna tabla no contenga ese campo.
-luego se combinana mediante un union all.

---

## 2. int_unified_returns

### OBJETIVO
Las tablas `store_returns`, `catalog_returns` y `web_returns` se han unificado
en `int_unified_returns`. Igual que el anterior pero para el tema de devoluciones.

### Decisiones de mapeo
- `return_date_sk` (store) → `returned_date_sk`, lo cambiamos para tener todos lo mismo.
- `return_time_sk` (store) → `returned_time_sk`, lo cambiamos para tener todos lo mismo.
- `ticket_number` (store) → `order_number`, lo cambiamos para tener todos lo mismo
- `web_page_sk` se utiliza como clave del canal web, ya que es la columna
  no esta disponible en los returns para que este igual esa parte tanto
  sales como returns.
-`customer`: aqui lo que hicimos fue cambiar en caso del store nuestro customer a refunded y añadir como null los clientes de returning.
-añadimos la sk dependiendo si es store, web o page.
- añadimos i.category para sacar la categoria del producto en el marts y añadimos el mes tambien.
### Reglas de unificación
- Tendremos el mismo orden de los campos en los 3 modelos.
-añadiremos nulls en caso de que alguna tabla no contenga ese campo.
-luego se combinana mediante un union all.

---

## 3. int_customer_enriched

### OBJETIVO
El modelo `int_customer_enriched` combina cuatro tablas relacionadas con el
cliente:
- `customer`
- `customer_address`
- `customer_demographics`
- `household_demographics`

Para tener todo lo relacionado con el cliente en una unica tabla.

### Decisiones de mapeo
-se utilizan las sk para joinear a otras tablas.
-utilizaremos algunas columnas no he sacado todas.

### Reglas de unificación
-mediante un left join uniremos las 4 tablas y sacaremos los campos
en un orden.


---
