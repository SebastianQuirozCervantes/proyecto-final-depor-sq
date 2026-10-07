-- Tabla de posiciones de la Liga Andina por temporada, calculada a partir de
-- TODOS los partidos finalizados (no solo los del club).
with partidos as (
    select * from {{ ref('stg_api__partidos') }}
    where estado = 'FINISHED' and codigo_competicion = 'LAN'
),

por_equipo as (
    select temporada_inicio, local_id as equipo_id, local_nombre as equipo,
           goles_local as gf, goles_visitante as gc
    from partidos
    union all
    select temporada_inicio, visitante_id, visitante_nombre,
           goles_visitante, goles_local
    from partidos
),

agregado as (
    select
        temporada_inicio,
        equipo_id,
        equipo,
        count(*)                  as partidos_jugados,
        count_if(gf > gc)         as ganados,
        count_if(gf = gc)         as empatados,
        count_if(gf < gc)         as perdidos,
        sum(gf)                   as goles_favor,
        sum(gc)                   as goles_contra,
        sum(gf) - sum(gc)         as diferencia_goles,
        3 * count_if(gf > gc) + count_if(gf = gc) as puntos
    from por_equipo
    group by temporada_inicio, equipo_id, equipo
)

select
    *,
    rank() over (
        partition by temporada_inicio
        order by puntos desc, diferencia_goles desc, goles_favor desc
    )                             as posicion,
    equipo_id = {{ var('club_id') }} as es_nuestro_club
from agregado
