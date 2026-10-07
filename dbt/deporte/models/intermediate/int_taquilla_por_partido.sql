-- Asistencia y recaudación por partido de local, a partir de las ventas.
with ventas as (
    select * from {{ ref('stg_taquilla__ventas') }}
),

aforo as (
    select sum(capacidad) as capacidad_estadio from {{ ref('tribunas') }}
)

select
    v.fecha_partido,
    v.codigo_competicion,
    sum(iff(v.es_venta_valida, v.cantidad, 0))                 as entradas_vendidas,
    sum(iff(not v.es_venta_valida, v.cantidad, 0))             as entradas_anuladas,
    sum(iff(v.es_venta_valida, v.importe, 0))                  as recaudacion_neta,
    sum(iff(v.es_venta_valida and v.canal in ('WEB', 'APP'), v.cantidad, 0))
        / nullif(sum(iff(v.es_venta_valida, v.cantidad, 0)), 0) as porcentaje_venta_digital,
    round(sum(iff(v.es_venta_valida, v.cantidad, 0)) / max(a.capacidad_estadio), 4) as ocupacion
from ventas v
cross join aforo a
group by v.fecha_partido, v.codigo_competicion
