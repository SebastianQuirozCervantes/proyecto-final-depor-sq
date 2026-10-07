{#
  Usa el schema personalizado tal cual (STAGING, INTERMEDIATE, MARTS, SEEDS)
  en lugar del comportamiento por defecto de dbt (<schema_target>_<custom>).
  Así los nombres coinciden con los schemas creados en snowflake/setup.sql,
  donde el rol del pipeline tiene permisos mínimos y explícitos.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema | upper }}
    {%- else -%}
        {{ custom_schema_name | trim | upper }}
    {%- endif -%}
{%- endmacro %}
