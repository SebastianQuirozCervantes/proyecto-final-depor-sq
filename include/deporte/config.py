"""Configuración del pipeline y su validación.

Los valores ajustables viven en la Variable de Airflow ``deporte_config``
(JSON). Aquí se definen los valores por defecto y se valida lo que llega, para
fallar pronto y con un mensaje claro si alguien la edita mal desde la UI.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from include.deporte.errores import ErrorConfiguracion

BUCKET_RAW = "deporte-raw"
BUCKET_REPORTES = "deporte-reportes"

CONN_API = "football_api"
CONN_MINIO = "minio_deporte"
CONN_SFTP = "sftp_rendimiento"
CONN_MYSQL = "mysql_taquilla"
CONN_SNOWFLAKE = "snowflake_deporte"

DIRECTORIO_SFTP_GPS = "/upload/gps"

VARIABLE_CONFIG = "deporte_config"
VARIABLE_MARCA_AGUA_TAQUILLA = "deporte_taquilla_marca_agua"

COMPETICIONES_SOPORTADAS = {"LAN", "CPA", "PL", "PD", "SA", "BL1", "FL1", "CL"}


@dataclass(frozen=True)
class ConfiguracionPipeline:
    competiciones: list[str] = field(default_factory=lambda: ["LAN", "CPA"])
    dias_ventana_api: int = 42
    max_archivos_por_corrida: int = 60
    umbral_filas_invalidas: float = 0.05
    club_id: int = 100


def cargar_configuracion(valor: dict | None) -> ConfiguracionPipeline:
    """Construye la configuración a partir del JSON de la Variable."""
    valor = dict(valor or {})
    desconocidas = set(valor) - set(ConfiguracionPipeline.__dataclass_fields__)
    if desconocidas:
        raise ErrorConfiguracion(f"Claves desconocidas en {VARIABLE_CONFIG}: {sorted(desconocidas)}")

    config = ConfiguracionPipeline(**valor)

    if not config.competiciones:
        raise ErrorConfiguracion("Debe haber al menos una competición configurada.")
    invalidas = [c for c in config.competiciones if c not in COMPETICIONES_SOPORTADAS]
    if invalidas:
        raise ErrorConfiguracion(f"Competiciones no soportadas: {invalidas}")
    if not 1 <= config.dias_ventana_api <= 120:
        raise ErrorConfiguracion("dias_ventana_api debe estar entre 1 y 120.")
    if not 0 <= config.umbral_filas_invalidas <= 0.5:
        raise ErrorConfiguracion("umbral_filas_invalidas debe estar entre 0 y 0.5.")
    if config.max_archivos_por_corrida < 1:
        raise ErrorConfiguracion("max_archivos_por_corrida debe ser >= 1.")
    return config
