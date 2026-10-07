-- Partidos finalizados vistos desde el Club Deportivo SQ: condición,
-- goles a favor/en contra, resultado y puntos obtenidos.
with partidos as (
    select * from {{ ref('stg_api__partidos') }}
    where estado = 'FINISHED'
      and {{ var('club_id') }} in (local_id, visitante_id)
)

select
    partido_id,
    codigo_competicion,
    temporada_inicio,
    jornada,
    fecha_partido,
    local_id = {{ var('club_id') }}                                              as es_local,
    iff(local_id = {{ var('club_id') }}, visitante_nombre, local_nombre)         as rival,
    iff(local_id = {{ var('club_id') }}, goles_local, goles_visitante)           as goles_favor,
    iff(local_id = {{ var('club_id') }}, goles_visitante, goles_local)           as goles_contra,
    case
        when goles_favor > goles_contra then 'G'
        when goles_favor = goles_contra then 'E'
        else 'P'
    end                                                                         as resultado,
    case resultado when 'G' then 3 when 'E' then 1 else 0 end                   as puntos
from partidos
