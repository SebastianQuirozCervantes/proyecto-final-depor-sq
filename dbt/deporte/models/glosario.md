{% docs fct_rendimiento_partido %}
Una fila por partido **finalizado** del Club Deportivo SQ. Cruza las tres
fuentes del pipeline: el resultado (API de partidos), la carga física del
plantel en la semana previa (archivos GPS) y la asistencia y recaudación del
partido cuando el club fue local (taquilla).
{% enddocs %}

{% docs resultado %}
Resultado desde la perspectiva del club: **G** = ganado (3 puntos),
**E** = empatado (1 punto), **P** = perdido (0 puntos).
{% enddocs %}

{% docs carga_interna %}
**Carga interna (sRPE)**: esfuerzo percibido (RPE, escala 1-10) multiplicado
por los minutos de la sesión. Se mide en unidades arbitrarias (UA).
{% enddocs %}

{% docs acwr %}
**ACWR (Acute:Chronic Workload Ratio)**: carga promedio de los últimos 7 días
dividida por la carga promedio de los últimos 28 días. Entre 0.8 y 1.3 es la
zona óptima; por encima de 1.5 el riesgo de lesión aumenta. Requiere al menos
21 días de historia.
{% enddocs %}

{% docs ocupacion %}
**Ocupación**: entradas pagadas (no anuladas) divididas entre el aforo
oficial del estadio (suma de las tribunas). Valor entre 0 y 1.
{% enddocs %}

{% docs recaudacion_neta %}
**Recaudación neta**: suma de cantidad × precio de las ventas en estado
PAGADA, en soles (PEN). Las ventas anuladas no suman.
{% enddocs %}
