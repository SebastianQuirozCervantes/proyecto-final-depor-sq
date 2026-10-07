-- Dimensión de jugadores del plantel con edad calculada.
select
    jugador_id,
    nombre,
    posicion,
    case posicion
        when 'POR' then 'Portero'
        when 'DEF' then 'Defensa'
        when 'MED' then 'Mediocampista'
        when 'DEL' then 'Delantero'
    end                                                     as posicion_descripcion,
    dorsal,
    fecha_nacimiento,
    datediff(year, fecha_nacimiento, current_date())
        - iff(dateadd(year, datediff(year, fecha_nacimiento, current_date()), fecha_nacimiento) > current_date(), 1, 0)
                                                            as edad
from {{ ref('jugadores') }}
