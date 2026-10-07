-- Hecho principal: una fila por partido finalizado del club, cruzando las TRES
-- fuentes: resultado (API), carga física previa del plantel (GPS) y
-- asistencia/recaudación (taquilla). Responde "¿llegamos frescos?, ¿ganamos?,
-- ¿cuánto recaudamos?" en una sola tabla lista para un dashboard.
with partidos as (
    select * from {{ ref('int_partidos_club') }}
),

carga as (
    select * from {{ ref('int_carga_diaria_jugador') }}
),

carga_previa as (
    -- Carga total del plantel en los 7 días ANTERIORES al partido.
    select
        p.partido_id,
        sum(c.carga_dia_ua)                                     as carga_equipo_7d_previa,
        count(distinct iff(c.tuvo_sesion, c.jugador_id, null))  as jugadores_con_sesion_7d
    from partidos p
    join carga c
        on c.fecha between dateadd(day, -7, p.fecha_partido) and dateadd(day, -1, p.fecha_partido)
    group by p.partido_id
),

riesgo_previo as (
    -- Jugadores en riesgo ALTO el día anterior al partido.
    select p.partido_id, count_if(c.nivel_riesgo = 'ALTO') as jugadores_riesgo_alto
    from partidos p
    join carga c on c.fecha = dateadd(day, -1, p.fecha_partido)
    group by p.partido_id
),

taquilla as (
    select * from {{ ref('int_taquilla_por_partido') }}
)

select
    p.partido_id,
    p.fecha_partido,
    p.codigo_competicion                                      as competicion,
    p.temporada_inicio,
    p.jornada,
    p.rival,
    p.es_local,
    p.goles_favor,
    p.goles_contra,
    p.resultado,
    p.puntos,
    cp.carga_equipo_7d_previa,
    cp.jugadores_con_sesion_7d,
    rp.jugadores_riesgo_alto,
    t.entradas_vendidas,
    t.ocupacion,
    t.recaudacion_neta,
    t.porcentaje_venta_digital,
    -- Rendimiento acumulado de la temporada (para la curva de puntos).
    sum(p.puntos) over (
        partition by p.codigo_competicion, p.temporada_inicio
        order by p.fecha_partido
        rows between unbounded preceding and current row
    )                                                         as puntos_acumulados_temporada
from partidos p
left join carga_previa cp on cp.partido_id = p.partido_id
left join riesgo_previo rp on rp.partido_id = p.partido_id
left join taquilla t
    on t.fecha_partido = p.fecha_partido
   and t.codigo_competicion = p.codigo_competicion
   and p.es_local
