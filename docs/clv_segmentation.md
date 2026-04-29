## CLV Segmentation

La segmentación de clientes se basa en percentiles del Customer Lifetime Value (CLV) calculados sobre el dataset TPC-DS.

Los puntos de corte obtenidos mediante `PERCENTILE_CONT` fueron:

- p33 = 425.83
- p66 = 1854.956

ponemos esos nombres ya que en 3 grupos por lo que son de 0 a 33, de 33 a 66 y de 66 a 100.

Con esta sentencia en sql snoflake:
select
    percentile_cont(0.33) within group (order by clv) as p33,
    percentile_cont(0.66) within group (order by clv) as p66
from (
    select
        bill_customer_sk as customer_sk,
        sum(net_paid) as clv
    from int_unified_sales
    group by 1
);

A partir de estos valores se definen los segmentos:

- **Low value**: CLV < 425.83
- **Medium value**: 425.83 ≤ CLV < 1854.956
- **High value**: CLV ≥ 1854.956

Esta segmentación divide la base de clientes en tres grupos equilibrados y permite análisis de comportamiento y valor.
