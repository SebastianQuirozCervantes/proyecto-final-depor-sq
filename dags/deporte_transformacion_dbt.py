"""
### DeporData · Transformación con dbt (Cosmos) y reporte diario

**Propósito:** convertir los datos crudos de `RAW` en modelos analíticos
versionados y testeados, y publicar el reporte diario para el cuerpo técnico.

**Cómo se dispara:** lo dispara `deporte_ingesta_diaria` al terminar la
ingesta (TriggerDagRunOperator). También se puede lanzar a mano para
re-procesar sin volver a ingerir.

**Qué hace:**
1. `transformacion_dbt` — Astronomer Cosmos (`DbtTaskGroup`) convierte cada
   seed, modelo y test del proyecto `dbt/deporte` en tareas nativas de Airflow:
   `seeds → staging → intermediate → marts`, cada modelo seguido de sus tests.
   Si un test con severidad *error* falla, los modelos que dependen de él no
   se construyen.
2. `generar_reporte_diario` — consulta los marts y deja un reporte Markdown en
   `s3://deporte-reportes/diario/fecha=YYYY-MM-DD/reporte.md`.

**Credenciales:** dbt lee Snowflake de variables de entorno con `env_var()`
(`dbt/deporte/profiles.yml`); el reporte usa la Connection `snowflake_deporte`.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import LoadMode, TestBehavior

from include.deporte.config import BUCKET_REPORTES, CONN_MINIO, CONN_SNOWFLAKE
from include.deporte.reportes import generar_reporte_markdown

log = logging.getLogger(__name__)

RAIZ = Path(__file__).resolve().parent.parent
DBT_PROYECTO = RAIZ / "dbt" / "deporte"
DBT_EJECUTABLE = Path("/opt/airflow/dbt_venv/bin/dbt")

perfil = ProfileConfig(
    profile_name="deporte",
    target_name="dev",
    profiles_yml_filepath=DBT_PROYECTO / "profiles.yml",
)

# Dentro del contenedor se usa `dbt ls` (fiel al proyecto real). Donde dbt no
# está instalado (por ejemplo, el test de integridad de DAGs en CI) Cosmos usa
# su parser propio, así el DAG se puede importar igual.
METODO_CARGA = LoadMode.DBT_LS if DBT_EJECUTABLE.exists() else LoadMode.CUSTOM

default_args = {
    "owner": "equipo_datos_deporte",
    # dbt contra Snowflake: un reintento cubre un corte de red puntual; más
    # reintentos solo repetirían un error de SQL o un test fallido.
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=45),
}


@dag(
    dag_id="deporte_transformacion_dbt",
    description="dbt (Cosmos) sobre Snowflake: RAW -> staging -> intermediate -> marts + reporte",
    schedule=None,  # se encadena desde deporte_ingesta_diaria
    start_date=pendulum.datetime(2026, 9, 1, tz="America/Lima"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    doc_md=__doc__,
    tags=["deporte", "dbt", "cosmos", "produccion"],
)
def deporte_transformacion_dbt():
    transformacion = DbtTaskGroup(
        group_id="transformacion_dbt",
        project_config=ProjectConfig(DBT_PROYECTO, install_dbt_deps=False, env_vars={"DBT_TARGET": "dev"}),
        profile_config=perfil,
        execution_config=ExecutionConfig(dbt_executable_path=str(DBT_EJECUTABLE)),
        render_config=RenderConfig(
            load_method=METODO_CARGA,
            test_behavior=TestBehavior.AFTER_EACH,
            # Los tests singulares que cruzan varios modelos (p. ej. ventas vs.
            # aforo de tribunas) se ejecutan como tareas propias al final.
            should_detach_multiple_parents_tests=True,
            dbt_executable_path=str(DBT_EJECUTABLE),
        ),
        operator_args={"full_refresh": False},
        default_args={"owner": "equipo_datos_deporte"},
    )

    @task(owner="analitica_deportiva")
    def generar_reporte_diario(dag_run=None, ds=None) -> str:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        fecha = (dag_run.conf or {}).get("fecha_datos", ds)
        hook = SnowflakeHook(snowflake_conn_id=CONN_SNOWFLAKE)

        def consultar(sql: str) -> list[dict]:
            df = hook.get_pandas_df(sql)
            df.columns = [c.upper() for c in df.columns]
            return df.to_dict("records")

        partidos = consultar(
            "select * from MARTS.FCT_RENDIMIENTO_PARTIDO "
            "where fecha_partido >= dateadd(day, -14, current_date()) order by fecha_partido desc"
        )
        riesgo = consultar("select * from MARTS.MART_RIESGO_LESION order by acwr desc nulls last")

        reporte = generar_reporte_markdown(fecha, partidos, riesgo)
        clave = f"diario/fecha={fecha}/reporte.md"
        S3Hook(aws_conn_id=CONN_MINIO).load_string(reporte, clave, bucket_name=BUCKET_REPORTES, replace=True)
        log.info("Reporte publicado en s3://%s/%s\n%s", BUCKET_REPORTES, clave, reporte)
        return clave

    transformacion >> generar_reporte_diario()


deporte_transformacion_dbt()
