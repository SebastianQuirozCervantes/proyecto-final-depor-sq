# CI/CD, Branch Protection, Secrets y Environments

Flujo de trabajo del equipo (Clase 6): **rama → Pull Request → CI en verde → merge → CD**.

## 1. Flujo de trabajo diario

```bash
git checkout main && git pull
git checkout -b feature/umbral-acwr       # una rama por cambio
# ... cambios + tests ...
pytest tests/unit && ruff check . && ruff format --check .
git commit -m "feat(dbt): umbral de ACWR configurable"
git push -u origin feature/umbral-acwr
```

Luego abrir el Pull Request hacia `main` en GitHub. El workflow **CI** corre
solo; con la protección de rama activa, el botón *Merge* queda bloqueado hasta
que todos los checks estén en verde.

## 2. Checks del workflow CI (`.github/workflows/ci.yml`)

| Job | Qué valida |
|---|---|
| `Lint + tests de lógica de negocio` | `ruff check`, `ruff format --check`, `pytest tests/unit` |
| `Integridad de DAGs (Airflow)` | Instala Airflow 2.10.5 con constraints oficiales y corre `pytest tests/dags` |
| `dbt parse (sin conexión a Snowflake)` | Que el proyecto dbt compile (refs, sources, YAML, macros) |
| `docker compose config + build` | Que `docker-compose.yaml` sea válido y la imagen de Airflow construya |

## 3. Branch Protection Rule (obligatoria)

En GitHub: **Settings → Branches → Add branch ruleset** (o *Add classic branch protection rule*):

1. *Branch name pattern*: `main`.
2. ✅ **Require a pull request before merging** (opcional: 1 aprobación).
3. ✅ **Require status checks to pass before merging** → ✅ *Require branches to be up to date* y agregar los checks:
   - `Lint + tests de lógica de negocio`
   - `Integridad de DAGs (Airflow)`
   - `dbt parse (sin conexión a Snowflake)`
   - `docker compose config + build`

   (Los checks aparecen en el buscador después de que el workflow corrió al menos una vez.)
4. ✅ **Block force pushes** y ✅ *Do not allow bypassing the above settings*.
5. Guardar.

Para comprobarla: abrir un PR que rompa un test → el merge queda bloqueado.

## 4. Secrets y Environment para el CD

El workflow **CD** (`.github/workflows/cd.yml`) ejecuta `dbt build` contra
Snowflake al mezclar a `main`.

1. **Settings → Environments → New environment** → `produccion`.
   - Opcional: *Required reviewers* (alguien debe aprobar cada despliegue) y *Deployment branches: main*.
2. Dentro del Environment, **Add environment secret**:
   - `SNOWFLAKE_ACCOUNT`
   - `SNOWFLAKE_USER` (`DEPORTE_PIPELINE_USER`)
   - `SNOWFLAKE_PASSWORD`
3. **Settings → Secrets and variables → Actions → Variables → New repository variable**:
   `CD_SNOWFLAKE_HABILITADO` = `true` (sin esta variable el job de CD se salta,
   así el repositorio funciona aunque no se hayan configurado los Secrets).

Los Secrets nunca aparecen en los logs (GitHub los enmascara) y solo están
disponibles para jobs que declaran `environment: produccion`.

## 5. Verificaciones antes de entregar

```bash
git log --oneline                 # historial incremental real
git log --all -- .env             # no debe mostrar nada
git ls-files | grep -i "\.env$"   # no debe mostrar nada
```
