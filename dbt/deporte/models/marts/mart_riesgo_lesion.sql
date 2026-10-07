-- Semáforo de riesgo de lesión: la foto MÁS RECIENTE de cada jugador.
-- Es lo primero que mira el preparador físico antes del entrenamiento.
with ultimo_dia as (
    select *
    from {{ ref('int_carga_diaria_jugador') }}
    qualify row_number() over (partition by jugador_id order by fecha desc) = 1
)

select
    u.fecha                     as fecha_referencia,
    j.jugador_id,
    j.nombre,
    j.posicion,
    j.dorsal,
    u.carga_aguda_7d,
    u.carga_cronica_28d,
    u.acwr,
    u.nivel_riesgo,
    case u.nivel_riesgo
        when 'ALTO'       then 'Reducir carga: priorizar recuperación y sesiones de baja intensidad.'
        when 'MODERADO'   then 'Vigilar: no aumentar la carga esta semana.'
        when 'BAJA_CARGA' then 'Subcargado: aumentar progresivamente para evitar desacondicionamiento.'
        when 'OPTIMO'     then 'Mantener la planificación actual.'
        else 'Sin historia suficiente para calcular el ACWR.'
    end                         as recomendacion
from ultimo_dia u
join {{ ref('dim_jugadores') }} j on j.jugador_id = u.jugador_id
