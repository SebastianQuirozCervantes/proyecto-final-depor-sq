-- Serie diaria y CONTINUA de carga por jugador (los días sin sesión cuentan
-- como carga 0) y el ratio carga aguda:crónica (ACWR).
--
--   carga aguda   = promedio de carga diaria de los últimos 7 días
--   carga crónica = promedio de carga diaria de los últimos 28 días
--   ACWR          = aguda / crónica
--
-- La literatura de ciencias del deporte (Gabbett, 2016) asocia un ACWR > 1.5
-- con mayor riesgo de lesión y la franja 0.8 - 1.3 con la "zona óptima".
with sesiones as (
    select jugador_id, fecha, sum(carga_interna_ua) as carga_dia_ua, boolor_agg(tipo_sesion = 'PARTIDO') as jugo_partido
    from {{ ref('stg_gps__carga_fisica') }}
    group by jugador_id, fecha
),

limites as (
    select min(fecha) as fecha_min, max(fecha) as fecha_max from sesiones
),

calendario as (
    select dateadd(day, row_number() over (order by seq4()) - 1, l.fecha_min) as fecha
    from table(generator(rowcount => 3660))
    cross join limites l
    qualify fecha <= l.fecha_max
),

serie as (
    select
        j.jugador_id,
        c.fecha,
        coalesce(s.carga_dia_ua, 0)        as carga_dia_ua,
        coalesce(s.jugo_partido, false)    as jugo_partido,
        s.jugador_id is not null           as tuvo_sesion
    from calendario c
    cross join {{ ref('jugadores') }} j
    left join sesiones s
        on s.jugador_id = j.jugador_id
       and s.fecha = c.fecha
),

ventanas as (
    select
        *,
        avg(carga_dia_ua) over (partition by jugador_id order by fecha rows between 6 preceding and current row)  as carga_aguda_7d,
        avg(carga_dia_ua) over (partition by jugador_id order by fecha rows between 27 preceding and current row) as carga_cronica_28d,
        row_number() over (partition by jugador_id order by fecha)                                               as dias_de_historia
    from serie
)

select
    jugador_id,
    fecha,
    carga_dia_ua,
    jugo_partido,
    tuvo_sesion,
    round(carga_aguda_7d, 1)    as carga_aguda_7d,
    round(carga_cronica_28d, 1) as carga_cronica_28d,
    dias_de_historia,
    iff(dias_de_historia >= {{ var('dias_minimos_acwr') }},
        round(ventanas.carga_aguda_7d / nullif(ventanas.carga_cronica_28d, 0), 2),
        null)                   as acwr,
    case
        when acwr is null then 'SIN_HISTORIA'
        when acwr > 1.5   then 'ALTO'
        when acwr > 1.3   then 'MODERADO'
        when acwr < 0.8   then 'BAJA_CARGA'
        else 'OPTIMO'
    end                         as nivel_riesgo
from ventanas
