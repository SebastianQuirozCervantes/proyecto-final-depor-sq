-- En cada temporada, los goles a favor de todos los equipos deben igualar a
-- los goles en contra, y las victorias a las derrotas.
select temporada_inicio
from {{ ref('mart_tabla_posiciones') }}
group by temporada_inicio
having sum(goles_favor) <> sum(goles_contra)
    or sum(ganados) <> sum(perdidos)
