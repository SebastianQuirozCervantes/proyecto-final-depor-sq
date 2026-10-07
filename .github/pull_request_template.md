## Qué cambia

<!-- Breve descripción del cambio y por qué. -->

## Cómo se probó

- [ ] `pytest tests/unit`
- [ ] `ruff check . && ruff format --check .`
- [ ] Si toca dbt: `dbt build` en local / clon de Snowflake
- [ ] Si toca un DAG: corrida manual en Airflow local

## Checklist

- [ ] Sin credenciales en el código ni en archivos versionados
- [ ] Documentación (README / `doc_md` / `schema.yml`) actualizada si aplica
