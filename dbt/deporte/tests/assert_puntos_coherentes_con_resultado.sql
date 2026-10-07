-- Regla de negocio: 3 puntos por victoria, 1 por empate, 0 por derrota, y el
-- resultado debe ser coherente con el marcador.
select partido_id, goles_favor, goles_contra, resultado, puntos
from {{ ref('fct_rendimiento_partido') }}
where (resultado = 'G' and (puntos <> 3 or goles_favor <= goles_contra))
   or (resultado = 'E' and (puntos <> 1 or goles_favor <> goles_contra))
   or (resultado = 'P' and (puntos <> 0 or goles_favor >= goles_contra))
