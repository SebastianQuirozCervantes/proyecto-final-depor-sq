-- Partidos tipados y deduplicados: la API se re-extrae en ventana, así que un
-- mismo partido llega varias veces; se conserva su versión más reciente.
with fuente as (
    select * from {{ source('raw', 'api_partidos') }}
),

tipado as (
    select
        try_to_number(match_id)                                                   as partido_id,
        competicion                                                               as codigo_competicion,
        try_to_date(temporada_inicio)                                             as temporada_inicio,
        try_to_number(jornada)                                                    as jornada,
        try_to_timestamp_ntz(fecha_utc, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')             as inicio_utc,
        upper(estado)                                                             as estado,
        try_to_number(local_id)                                                   as local_id,
        local_nombre,
        try_to_number(visitante_id)                                               as visitante_id,
        visitante_nombre,
        try_to_number(goles_local)                                                as goles_local,
        try_to_number(goles_visitante)                                            as goles_visitante,
        ganador,
        try_to_timestamp_ntz(ultima_actualizacion, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')  as ultima_actualizacion,
        try_to_timestamp_ntz(_cargado_en, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')           as cargado_en
    from fuente
)

select
    *,
    -- Fecha del partido en hora de Lima (la que usan GPS y taquilla).
    convert_timezone('UTC', 'America/Lima', inicio_utc)::date as fecha_partido
from tipado
qualify row_number() over (
    partition by partido_id
    order by ultima_actualizacion desc nulls last, cargado_en desc
) = 1
