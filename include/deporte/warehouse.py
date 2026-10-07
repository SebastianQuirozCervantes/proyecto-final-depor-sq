"""Carga idempotente de lotes en la capa RAW de Snowflake.

Recibe una conexión DB-API y la función de escritura (``write_pandas`` en
producción), así que no depende de Airflow y se prueba con dobles en pytest.
"""

from __future__ import annotations

from collections.abc import Callable

TABLAS_RAW = {"API_PARTIDOS", "GPS_CARGA_FISICA", "TAQUILLA_VENTAS"}


def cargar_lote_idempotente(
    conexion,
    tabla: str,
    lote_id: str,
    filas: list[dict],
    escribir: Callable[[object, list[dict], str], None],
) -> int:
    """Borra el lote (si existía) y lo vuelve a insertar.

    Re-ejecutar una tarea (reintento, clear manual o backfill) deja exactamente
    el mismo resultado: nunca se duplican filas en RAW.
    """
    if tabla not in TABLAS_RAW:
        # El nombre de tabla no se puede parametrizar en SQL: lista blanca.
        raise ValueError(f"Tabla RAW desconocida: {tabla}")
    cursor = conexion.cursor()
    try:
        cursor.execute(f"DELETE FROM RAW.{tabla} WHERE _LOTE_ID = %s", (lote_id,))
    finally:
        cursor.close()
    if filas:
        escribir(conexion, filas, tabla)
    return len(filas)
