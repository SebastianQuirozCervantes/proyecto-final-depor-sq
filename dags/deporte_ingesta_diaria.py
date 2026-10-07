"""
### DeporData · Ingesta diaria del Club Deportivo SQ

**Propósito:** reunir cada mañana, en un solo lugar, los tres datos que el
cuerpo técnico y la gerencia del club necesitan cruzar:

| Fuente | Tipo | Qué trae |
|---|---|---|
| API football-data.org (v4) | API REST con token y límite de tasa | Partidos y resultados de Liga y Copa |
| SFTP del proveedor de chalecos GPS | Archivo CSV diario | Carga física por jugador y sesión |
| MySQL heredado de taquilla | Base de datos on-premise | Venta de entradas por partido |

**Flujo:** `fuente → MinIO (zona raw, trazabilidad) → Snowflake RAW → dbt`.

1. `ingesta_api_partidos`: Dynamic Task Mapping (una tarea por competición),
   re-extrae una ventana de días para capturar marcadores corregidos.
2. `ingesta_sftp_gps`: un sensor (`mode="reschedule"`) espera el archivo del día;
   luego se procesan **todos** los archivos pendientes (mapping por archivo),
   las filas inválidas van a `cuarentena/` en MinIO.
3. `ingesta_mysql_taquilla`: extracción incremental por marca de agua
   (`updated_at`); la marca solo avanza después de cargar en Snowflake.
4. Al terminar se dispara `deporte_transformacion_dbt` (TriggerDagRunOperator).

**Credenciales:** todas en Connections (`football_api`, `sftp_rendimiento`,
`mysql_taquilla`, `minio_deporte`, `snowflake_deporte`). Configuración en la
Variable JSON `deporte_config` (opcional; ver README).

**Owner:** equipo de datos del club · **Consumidores:** cuerpo técnico, gerencia comercial.
"""

from __future__ import annotations

import logging
from dataclasses import asdict
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException, AirflowFailException, AirflowSkipException
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from include.deporte import carga_fisica, lotes, partidos, taquilla
from include.deporte.config import (
    BUCKET_RAW,
    CONN_API,
    CONN_MINIO,
    CONN_MYSQL,
    CONN_SFTP,
    CONN_SNOWFLAKE,
    DIRECTORIO_SFTP_GPS,
    VARIABLE_CONFIG,
    VARIABLE_MARCA_AGUA_TAQUILLA,
    cargar_configuracion,
)
from include.deporte.errores import ErrorDefinitivo, ErrorReintentable

log = logging.getLogger(__name__)

ZONA_HORARIA = pendulum.timezone("America/Lima")
PREFIJO_CONTROL_GPS = "gps/_control/cargados"
RUTA_INCLUDE = str(Path(__file__).resolve().parent.parent / "include")


def alertar_fallo(context) -> None:
    """Callback de observabilidad: aquí se conectaría Slack/Teams/email."""
    ti = context["task_instance"]
    log.error(
        "ALERTA DeporData | dag=%s tarea=%s intento=%s fecha=%s log=%s",
        ti.dag_id,
        ti.task_id,
        ti.try_number,
        context["ds"],
        ti.log_url,
    )


def _ejecutar(funcion, *args, **kwargs):
    """Traduce los errores del dominio a la semántica de reintentos de Airflow."""
    try:
        return funcion(*args, **kwargs)
    except ErrorDefinitivo as error:
        # Reintentar no arregla un token inválido ni un archivo corrupto.
        raise AirflowFailException(f"[sin reintento] {error}") from error
    except ErrorReintentable as error:
        raise AirflowException(f"[se reintentará] {error}") from error


def _s3():
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    return S3Hook(aws_conn_id=CONN_MINIO)


def _cargar_raw(tabla: str, lote_id: str, filas: list[dict]) -> int:
    import pandas as pd
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    from include.deporte.warehouse import cargar_lote_idempotente
    from snowflake.connector.pandas_tools import write_pandas

    def escribir(conexion, filas_lote, nombre_tabla):
        resultado = write_pandas(
            conexion, pd.DataFrame(filas_lote), nombre_tabla, schema="RAW", quote_identifiers=False
        )
        if not resultado[0]:
            raise AirflowException(f"write_pandas no pudo cargar {nombre_tabla}: {resultado}")

    hook = SnowflakeHook(snowflake_conn_id=CONN_SNOWFLAKE)
    with hook.get_conn() as conexion:
        total = cargar_lote_idempotente(conexion, tabla, lote_id, filas, escribir)
    log.info("Snowflake RAW.%s: lote %s cargado con %s filas", tabla, lote_id, total)
    return total


def _ahora_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


default_args = {
    "owner": "equipo_datos_deporte",
    # Fuentes externas: fallos transitorios (red, 429, SFTP ocupado) son
    # esperables. 3 reintentos con backoff exponencial (2, 4, 8 min, tope 20)
    # cubren una caída breve sin martillar a la fuente.
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": alertar_fallo,
}


@dag(
    dag_id="deporte_ingesta_diaria",
    description="Ingesta diaria: API de partidos + SFTP GPS + MySQL taquilla -> MinIO -> Snowflake",
    # 06:00 hora de Lima: el proveedor GPS ya entregó el archivo del día anterior
    # y el cuerpo técnico tiene el reporte antes del entrenamiento (09:00).
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz=ZONA_HORARIA),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=8),
    default_args=default_args,
    doc_md=__doc__,
    template_searchpath=[RUTA_INCLUDE],
    tags=["deporte", "ingesta", "produccion"],
)
def deporte_ingesta_diaria():
    @task(retries=0)
    def leer_configuracion() -> dict:
        """Lee y valida la Variable `deporte_config` (si no existe usa defaults)."""
        valor = Variable.get(VARIABLE_CONFIG, default_var={}, deserialize_json=True)
        return asdict(_ejecutar(cargar_configuracion, valor))

    config = leer_configuracion()

    preparar_tablas_raw = SQLExecuteQueryOperator(
        task_id="preparar_tablas_raw",
        conn_id=CONN_SNOWFLAKE,
        sql="sql/raw_tablas.sql",
        split_statements=True,
        retries=2,
    )

    # ------------------------------------------------------------------ API
    with TaskGroup("ingesta_api_partidos", tooltip="API football-data.org -> MinIO -> Snowflake") as grupo_api:

        @task
        def listar_competiciones(cfg: dict) -> list[str]:
            return cfg["competiciones"]

        @task(owner="analitica_deportiva", map_index_template="{{ competicion }}")
        def extraer_partidos(competicion: str, cfg: dict, ds=None) -> str:
            """Extrae una competición en tramos de <=10 días y la guarda en MinIO."""
            import requests
            from airflow.hooks.base import BaseHook

            from include.deporte.api_client import ClienteFootballData

            conexion = BaseHook.get_connection(CONN_API)
            cliente = _ejecutar(ClienteFootballData, conexion.host, conexion.password, requests.Session())

            desde, hasta = partidos.ventana_extraccion(date.fromisoformat(ds), cfg["dias_ventana_api"])
            extraido_en = _ahora_iso()
            filas = []
            for tramo_desde, tramo_hasta in partidos.dividir_rango_fechas(desde, hasta):
                respuesta = _ejecutar(cliente.obtener_partidos, competicion, tramo_desde, tramo_hasta)
                filas += _ejecutar(partidos.aplanar_partidos, respuesta, competicion, extraido_en)
            filas = partidos.deduplicar_partidos(filas)

            clave = f"api/partidos/competicion={competicion}/fecha={ds}/partidos.jsonl"
            _s3().load_string(lotes.a_json_lines(filas), clave, bucket_name=BUCKET_RAW, replace=True)
            log.info(
                "%s: %s partidos entre %s y %s -> s3://%s/%s", competicion, len(filas), desde, hasta, BUCKET_RAW, clave
            )
            return clave

        @task(owner="analitica_deportiva")
        def cargar_partidos(claves: list[str], ds=None) -> int:
            filas = []
            for clave in claves:
                filas += lotes.desde_json_lines(_s3().read_key(clave, bucket_name=BUCKET_RAW))
            lote = lotes.preparar_lote(filas, partidos.COLUMNAS_PARTIDOS, f"api_{ds}", _ahora_iso())
            return _cargar_raw("API_PARTIDOS", f"api_{ds}", lote)

        claves = extraer_partidos.partial(cfg=config).expand(competicion=listar_competiciones(config))
        carga_api = cargar_partidos(claves)

    # ------------------------------------------------------------------ SFTP
    with TaskGroup("ingesta_sftp_gps", tooltip="SFTP proveedor GPS -> MinIO -> Snowflake") as grupo_gps:
        esperar_archivo = SFTPSensor(
            task_id="esperar_archivo_gps",
            sftp_conn_id=CONN_SFTP,
            path=DIRECTORIO_SFTP_GPS + "/carga_fisica_{{ ds_nodash }}.csv",
            # reschedule: libera el worker entre chequeos (no ocupa un slot 6 horas).
            mode="reschedule",
            poke_interval=10 * 60,
            timeout=6 * 60 * 60,
            retries=0,  # el propio timeout ya es la tolerancia; si vence, hay que avisar.
            owner="rendimiento_fisico",
        )

        @task(owner="rendimiento_fisico")
        def listar_pendientes(cfg: dict, ds=None) -> list[str]:
            """Archivos del SFTP que aún no tienen marca de carga en MinIO."""
            from airflow.providers.sftp.hooks.sftp import SFTPHook

            disponibles = SFTPHook(ssh_conn_id=CONN_SFTP).list_directory(DIRECTORIO_SFTP_GPS)
            marcas = _s3().list_keys(bucket_name=BUCKET_RAW, prefix=PREFIJO_CONTROL_GPS + "/") or []
            procesados = {m.rsplit("/", 1)[-1].removesuffix(".ok") for m in marcas}
            pendientes = carga_fisica.archivos_pendientes(
                disponibles, procesados, date.fromisoformat(ds), cfg["max_archivos_por_corrida"]
            )
            log.info("%s archivos en SFTP, %s pendientes de carga", len(disponibles), len(pendientes))
            return pendientes

        @task(owner="rendimiento_fisico", map_index_template="{{ archivo }}", max_active_tis_per_dagrun=4)
        def procesar_archivo_gps(archivo: str, cfg: dict) -> dict:
            """SFTP -> MinIO (crudo + cuarentena) -> validación -> Snowflake RAW."""
            from airflow.providers.sftp.hooks.sftp import SFTPHook

            fecha = carga_fisica.fecha_desde_nombre_archivo(archivo)
            hook = SFTPHook(ssh_conn_id=CONN_SFTP)
            try:
                with hook.get_conn().open(f"{DIRECTORIO_SFTP_GPS}/{archivo}", "r") as remoto:
                    contenido = remoto.read().decode("utf-8")
            finally:
                hook.close_conn()

            s3 = _s3()
            particion = f"fecha={fecha.isoformat()}"
            s3.load_string(contenido, f"gps/crudo/{particion}/{archivo}", bucket_name=BUCKET_RAW, replace=True)

            filas = _ejecutar(carga_fisica.leer_csv, contenido)
            validas, rechazadas = carga_fisica.validar_carga_fisica(filas, fecha)
            if rechazadas:
                s3.load_string(
                    lotes.a_json_lines(rechazadas),
                    f"cuarentena/gps/{particion}/{archivo}.rechazadas.jsonl",
                    bucket_name=BUCKET_RAW,
                    replace=True,
                )
                log.warning("%s: %s filas a cuarentena. Ejemplo: %s", archivo, len(rechazadas), rechazadas[0])
            proporcion = _ejecutar(
                carga_fisica.evaluar_calidad, len(validas), len(rechazadas), cfg["umbral_filas_invalidas"], archivo
            )

            lote = lotes.preparar_lote(
                carga_fisica.a_filas_raw(validas, archivo), carga_fisica.COLUMNAS_RAW, archivo, _ahora_iso()
            )
            _cargar_raw("GPS_CARGA_FISICA", archivo, lote)
            # La marca de control se escribe AL FINAL: si algo falla antes, el
            # archivo sigue pendiente y se reprocesa en la próxima corrida.
            s3.load_string(_ahora_iso(), f"{PREFIJO_CONTROL_GPS}/{archivo}.ok", bucket_name=BUCKET_RAW, replace=True)
            return {"archivo": archivo, "validas": len(validas), "rechazadas": len(rechazadas), "pct": proporcion}

        pendientes = listar_pendientes(config)
        esperar_archivo >> pendientes
        carga_gps = procesar_archivo_gps.partial(cfg=config).expand(archivo=pendientes)

    # ------------------------------------------------------------------ MySQL
    with TaskGroup(
        "ingesta_mysql_taquilla", tooltip="MySQL heredado -> MinIO -> Snowflake (incremental)"
    ) as grupo_taquilla:

        @task(owner="gerencia_comercial")
        def extraer_ventas(data_interval_end=None, ds=None) -> dict:
            from airflow.providers.mysql.hooks.mysql import MySqlHook

            marca = Variable.get(VARIABLE_MARCA_AGUA_TAQUILLA, default_var=taquilla.MARCA_AGUA_INICIAL)
            # La base heredada guarda la hora local de Lima, sin zona horaria.
            hasta = data_interval_end.in_timezone(ZONA_HORARIA).strftime(taquilla.FORMATO)
            if marca >= hasta:
                raise AirflowSkipException(f"Marca de agua {marca} ya cubre hasta {hasta}.")
            sql, parametros = taquilla.consulta_incremental(marca, hasta)
            filas = taquilla.normalizar_filas(
                MySqlHook(mysql_conn_id=CONN_MYSQL).get_records(sql, parameters=parametros)
            )
            if not filas:
                raise AirflowSkipException(f"Sin ventas nuevas o modificadas entre {marca} y {hasta}.")

            clave = f"mysql/taquilla/fecha={ds}/ventas_{marca[:10]}_{hasta[:10]}.jsonl"
            _s3().load_string(lotes.a_json_lines(filas), clave, bucket_name=BUCKET_RAW, replace=True)
            log.info("Taquilla: %s filas (%s, %s] -> %s", len(filas), marca, hasta, clave)
            return {"clave": clave, "nueva_marca": taquilla.nueva_marca_agua(filas, marca)}

        @task(owner="gerencia_comercial")
        def cargar_ventas(extraccion: dict, ds=None) -> str:
            filas = lotes.desde_json_lines(_s3().read_key(extraccion["clave"], bucket_name=BUCKET_RAW))
            lote_id = extraccion["clave"].rsplit("/", 1)[-1]
            _cargar_raw(
                "TAQUILLA_VENTAS", lote_id, lotes.preparar_lote(filas, taquilla.COLUMNAS_RAW, lote_id, _ahora_iso())
            )
            return extraccion["nueva_marca"]

        @task(owner="gerencia_comercial", retries=1)
        def actualizar_marca_agua(nueva_marca: str) -> None:
            # Solo se avanza la marca cuando los datos ya están en Snowflake:
            # garantía "al menos una vez" (dbt deduplica por venta_id).
            Variable.set(VARIABLE_MARCA_AGUA_TAQUILLA, nueva_marca)
            log.info("Nueva marca de agua de taquilla: %s", nueva_marca)

        carga_taquilla = actualizar_marca_agua(cargar_ventas(extraer_ventas()))

    fin_ingesta = EmptyOperator(task_id="fin_ingesta", trigger_rule=TriggerRule.ALL_DONE)

    # dbt se ejecuta aunque una fuente falle o no traiga novedades: los modelos
    # son idempotentes y los datos de las otras fuentes siguen siendo útiles.
    # El fallo NO queda oculto: `verificar_corrida` marca la corrida como fallida.
    disparar_transformacion = TriggerDagRunOperator(
        task_id="disparar_transformacion_dbt",
        trigger_dag_id="deporte_transformacion_dbt",
        conf={"fecha_datos": "{{ ds }}"},
        logical_date="{{ data_interval_end }}",
        reset_dag_run=True,
        wait_for_completion=False,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    @task(trigger_rule=TriggerRule.ALL_DONE, retries=0)
    def verificar_corrida(dag_run=None) -> None:
        fallidas = [ti.task_id for ti in dag_run.get_task_instances() if ti.state in ("failed", "upstream_failed")]
        if fallidas:
            raise AirflowFailException(f"Tareas con fallo en la corrida: {sorted(set(fallidas))}")

    config >> preparar_tablas_raw >> [grupo_api, grupo_gps, grupo_taquilla]
    [carga_api, carga_gps, carga_taquilla] >> fin_ingesta >> disparar_transformacion >> verificar_corrida()


deporte_ingesta_diaria()
