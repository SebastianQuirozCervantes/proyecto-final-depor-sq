-- Regla de negocio: en ningún partido se pueden vender más entradas válidas
-- que el aforo oficial de la tribuna (sería sobreventa o datos duplicados).
with vendidas as (
    select fecha_partido, tribuna, sum(cantidad) as entradas
    from {{ ref('stg_taquilla__ventas') }}
    where es_venta_valida
    group by fecha_partido, tribuna
)

select v.fecha_partido, v.tribuna, v.entradas, t.capacidad
from vendidas v
join {{ ref('tribunas') }} t on t.tribuna = v.tribuna
where v.entradas > t.capacidad
