{#
  Test genérico personalizado: la combinación de columnas debe ser única
  (equivalente a dbt_utils.unique_combination_of_columns, sin depender de
  paquetes externos).
#}
{% test combinacion_unica(model, columnas) %}
select {{ columnas | join(', ') }}, count(*) as repeticiones
from {{ model }}
group by {{ columnas | join(', ') }}
having count(*) > 1
{% endtest %}
