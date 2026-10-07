-- =====================================================================
-- Consultas de ejemplo de la capa de consumo (ejecutar en Snowflake).
-- Demuestran el valor de negocio de los marts.
-- =====================================================================

-- 1) ¿Rinde peor el equipo cuando llega sobrecargado?
--    Puntos promedio según la carga del plantel en la semana previa.
select
    case
        when carga_equipo_7d_previa < percentile_cont(0.33) within group (order by carga_equipo_7d_previa) over () then '1. Carga baja'
        when carga_equipo_7d_previa < percentile_cont(0.66) within group (order by carga_equipo_7d_previa) over () then '2. Carga media'
        else '3. Carga alta'
    end                                   as nivel_carga_previa,
    resultado,
    puntos
from {{ ref('fct_rendimiento_partido') }}
where carga_equipo_7d_previa is not null;

-- 2) Jugadores a cuidar en el entrenamiento de hoy.
select nombre, posicion, acwr, nivel_riesgo, recomendacion
from {{ ref('mart_riesgo_lesion') }}
where nivel_riesgo in ('ALTO', 'MODERADO')
order by acwr desc;

-- 3) ¿Ganar atrae más público? Ocupación del siguiente partido de local
--    según el resultado del partido anterior.
select
    lag(resultado) over (order by fecha_partido) as resultado_partido_anterior,
    fecha_partido,
    rival,
    ocupacion,
    recaudacion_neta
from {{ ref('fct_rendimiento_partido') }}
where competicion = 'LAN'
qualify es_local
order by fecha_partido;

-- 4) Posición del club en la tabla de la temporada actual.
select posicion, equipo, puntos, partidos_jugados, diferencia_goles
from {{ ref('mart_tabla_posiciones') }}
where temporada_inicio = (select max(temporada_inicio) from {{ ref('mart_tabla_posiciones') }})
order by posicion;
