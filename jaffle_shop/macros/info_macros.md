--
macro 1 calc_weeks_of_stock
##Propósito
calcular las semanas de stock diviendo el stock semanal entre las ventas evitando divisiones por 0
##Parámetros
stock :columna o expresión que representa el stock medio semanal
sales : columna o expresión que representa las ventas medias semanales
##Ejemplo de uso como invocarlo
{{ calc_weeks_of_stock('iw.avg_stock_week', 'sw.avg_sales_week') }} as weeks_of_stock

--
macro 2 classify_trend
##Propósito
calcular la tendencia temporal de una metrica comparando su valor actual con el de la semana anterior
##Parámetros
metric:columna numérica cuyo comportamiento quieres analizar
partition_by:clave de partición (por ejemplo, item_sk)
order_by:columna temporal que define el orden (por ejemplo, week)
##Ejemplo de uso como invocarlo
{{ classify_trend('iw.avg_stock_week', 'iw.item_sk', 'iw.week') }} as trend
