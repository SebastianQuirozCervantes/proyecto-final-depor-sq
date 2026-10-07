"""Tests de integridad de los DAGs (se ejecutan en CI con Airflow instalado).

No prueban Airflow en sí: verifican que NUESTROS DAGs cumplan las reglas de
diseño del proyecto (sin errores de importación, catchup desactivado, owners,
reintentos con criterio, sensores en modo reschedule, sin secretos en código).
"""

import re
from pathlib import Path

import pytest

pytest.importorskip("airflow")

from airflow.models import DagBag  # noqa: E402
from airflow.models.mappedoperator import MappedOperator  # noqa: E402
from airflow.sensors.base import BaseSensorOperator  # noqa: E402

RAIZ = Path(__file__).resolve().parents[2]
DAGS = RAIZ / "dags"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS), include_examples=False)


def test_los_dags_importan_sin_errores(dagbag):
    assert dagbag.import_errors == {}
    assert {"deporte_ingesta_diaria", "deporte_transformacion_dbt"} <= set(dagbag.dag_ids)


def test_dag_productivo_tiene_schedule_real_y_sin_catchup(dagbag):
    dag = dagbag.get_dag("deporte_ingesta_diaria")
    assert dag.timetable.summary == "0 6 * * *"
    assert dag.catchup is False
    assert dag.max_active_runs == 1


@pytest.mark.parametrize("dag_id", ["deporte_ingesta_diaria", "deporte_transformacion_dbt"])
def test_documentacion_y_owner(dagbag, dag_id):
    dag = dagbag.get_dag(dag_id)
    assert dag.doc_md and len(dag.doc_md) > 200
    for tarea in dag.tasks:
        assert tarea.owner and tarea.owner != "airflow", f"{tarea.task_id} sin owner"


def test_reintentos_configurados_con_criterio(dagbag):
    dag = dagbag.get_dag("deporte_ingesta_diaria")
    extraer = dag.get_task("ingesta_api_partidos.extraer_partidos")
    assert extraer.retries == 3
    assert extraer.retry_exponential_backoff is True
    assert extraer.max_retry_delay is not None
    # Validar configuración no se reintenta: un JSON mal escrito no se arregla solo.
    assert dag.get_task("leer_configuracion").retries == 0


def test_usa_task_groups_y_dynamic_task_mapping(dagbag):
    dag = dagbag.get_dag("deporte_ingesta_diaria")
    grupos = set(dag.task_group.children)
    assert {"ingesta_api_partidos", "ingesta_sftp_gps", "ingesta_mysql_taquilla"} <= grupos
    assert isinstance(dag.get_task("ingesta_api_partidos.extraer_partidos"), MappedOperator)
    assert isinstance(dag.get_task("ingesta_sftp_gps.procesar_archivo_gps"), MappedOperator)


def test_sensor_en_modo_reschedule_con_timeout(dagbag):
    dag = dagbag.get_dag("deporte_ingesta_diaria")
    sensores = [t for t in dag.tasks if isinstance(t, BaseSensorOperator)]
    assert sensores, "El DAG de ingesta debe esperar el archivo GPS con un sensor"
    for sensor in sensores:
        assert sensor.mode == "reschedule"
        assert sensor.timeout <= 8 * 60 * 60


def test_ingesta_dispara_la_transformacion(dagbag):
    dag = dagbag.get_dag("deporte_ingesta_diaria")
    disparo = dag.get_task("disparar_transformacion_dbt")
    assert disparo.trigger_dag_id == "deporte_transformacion_dbt"
    assert disparo.trigger_rule == "all_done"
    assert "verificar_corrida" in disparo.downstream_task_ids


def test_transformacion_ejecuta_dbt_con_cosmos(dagbag):
    dag = dagbag.get_dag("deporte_transformacion_dbt")
    ids = set(dag.task_ids)
    assert any(i.startswith("transformacion_dbt.fct_rendimiento_partido") for i in ids)
    assert any(i.startswith("transformacion_dbt.mart_riesgo_lesion") for i in ids)
    assert "generar_reporte_diario" in ids


PATRONES_SECRETOS = [
    re.compile(r"(password|passwd|secret|token|api_key)\s*=\s*['\"][^'\"]{4,}['\"]", re.IGNORECASE),
    re.compile(r"AKIA[0-9A-Z]{16}"),
]


@pytest.mark.parametrize("archivo", sorted(DAGS.glob("*.py")) + sorted((RAIZ / "include").rglob("*.py")))
def test_sin_credenciales_en_el_codigo(archivo):
    contenido = archivo.read_text(encoding="utf-8")
    for patron in PATRONES_SECRETOS:
        assert not patron.search(contenido), f"Posible credencial en {archivo.name}"
