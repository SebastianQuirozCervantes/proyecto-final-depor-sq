"""Transformaciones de la fuente 3: base MySQL heredada de taquilla.

Extracción incremental *batch* por marca de agua (``updated_at``): en cada
corrida solo se leen las filas creadas o modificadas desde la última corrida
exitosa. Así una venta ANULADA días después vuelve a viajar y dbt se queda con
su última versión (patrón visto en la Clase 8: batch incremental vs CDC).
"""

from __future__ import annotations

from datetime import datetime

MARCA_AGUA_INICIAL = "1970-01-01 00:00:00"
FORMATO = "%Y-%m-%d %H:%M:%S"

COLUMNAS_TAQUILLA = [
    "venta_id",
    "fecha_partido",
    "codigo_competicion",
    "tribuna",
    "canal",
    "cantidad",
    "precio_unitario",
    "estado",
    "created_at",
    "updated_at",
]

COLUMNAS_RAW = [c.upper() for c in COLUMNAS_TAQUILLA]


def consulta_incremental(marca_agua: str, hasta: str) -> tuple[str, tuple[str, str]]:
    """SQL parametrizado (nunca concatenar valores: evita inyección SQL).

    El límite superior ``hasta`` (fin del intervalo de datos de la corrida)
    hace la extracción reproducible: re-ejecutar una corrida trae lo mismo.
    """
    datetime.strptime(marca_agua, FORMATO)
    datetime.strptime(hasta, FORMATO)
    if marca_agua >= hasta:
        raise ValueError(f"La marca de agua {marca_agua} no es anterior a {hasta}")
    sql = (
        f"SELECT {', '.join(COLUMNAS_TAQUILLA)} FROM ventas_entradas "
        "WHERE updated_at > %s AND updated_at <= %s ORDER BY updated_at, venta_id"
    )
    return sql, (marca_agua, hasta)


def _texto(valor) -> str | None:
    if valor is None:
        return None
    if isinstance(valor, datetime):
        return valor.strftime(FORMATO)
    return str(valor)


def normalizar_filas(filas: list[tuple]) -> list[dict]:
    """Tuplas de MySQL -> dicts con nombres de columna RAW y valores texto."""
    normalizadas = []
    for fila in filas:
        if len(fila) != len(COLUMNAS_TAQUILLA):
            raise ValueError(f"Se esperaban {len(COLUMNAS_TAQUILLA)} columnas y llegaron {len(fila)}")
        normalizadas.append({col: _texto(v) for col, v in zip(COLUMNAS_RAW, fila, strict=False)})
    return normalizadas


def nueva_marca_agua(filas: list[dict], marca_actual: str) -> str:
    """La nueva marca es el mayor ``updated_at`` leído (nunca retrocede)."""
    if not filas:
        return marca_actual
    return max([marca_actual] + [f["UPDATED_AT"] for f in filas])
