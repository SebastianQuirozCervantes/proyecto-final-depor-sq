-- Carga física tipada. Agrega la carga interna por sesión (sRPE = RPE x minutos,
-- método de Foster), medida estándar en ciencias del deporte, en unidades
-- arbitrarias (UA).
with fuente as (
    select * from {{ source('raw', 'gps_carga_fisica') }}
),

tipado as (
    select
        try_to_date(fecha)                                       as fecha,
        trim(jugador_id)                                         as jugador_id,
        upper(tipo_sesion)                                       as tipo_sesion,
        try_to_number(minutos)                                   as minutos,
        try_to_decimal(distancia_total_m, 10, 1)                 as distancia_total_m,
        try_to_decimal(distancia_alta_intensidad_m, 10, 1)       as distancia_alta_intensidad_m,
        try_to_number(sprints)                                   as sprints,
        try_to_decimal(velocidad_max_kmh, 5, 1)                  as velocidad_max_kmh,
        try_to_number(fc_media_lpm)                              as fc_media_lpm,
        try_to_number(rpe)                                       as rpe,
        archivo_origen,
        try_to_timestamp_ntz(_cargado_en, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as cargado_en
    from fuente
)

select
    to_char(fecha, 'YYYYMMDD') || '-' || jugador_id || '-' || tipo_sesion as sesion_jugador_id,
    *,
    rpe * minutos as carga_interna_ua,
    round(distancia_total_m / nullif(minutos, 0), 1) as metros_por_minuto
from tipado
qualify row_number() over (
    partition by fecha, jugador_id, tipo_sesion
    order by cargado_en desc
) = 1
