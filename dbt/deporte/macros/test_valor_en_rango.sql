{#
  Test genérico personalizado: falla si algún valor (no nulo) cae fuera de
  [minimo, maximo]. Se usa para rangos fisiológicos y de negocio.
#}
{% test valor_en_rango(model, column_name, minimo, maximo) %}
select {{ column_name }} as valor_fuera_de_rango
from {{ model }}
where {{ column_name }} is not null
  and ({{ column_name }} < {{ minimo }} or {{ column_name }} > {{ maximo }})
{% endtest %}
