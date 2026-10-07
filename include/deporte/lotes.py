"""Preparación de lotes para la carga en la capa RAW de Snowflake.

Cada fila se carga con dos columnas de linaje:
* ``_LOTE_ID``: identifica el lote (archivo, fecha de corrida...). Permite que
  la carga sea idempotente: antes de insertar se borra el mismo lote.
* ``_CARGADO_EN``: cuándo se cargó, para auditoría y para deduplicar en dbt.
"""

from __future__ import annotations

import json


def preparar_lote(filas: list[dict], columnas: list[str], lote_id: str, cargado_en: str) -> list[dict]:
    """Normaliza las filas al esquema RAW (todo texto, columnas en orden)."""
    if not lote_id:
        raise ValueError("lote_id es obligatorio para garantizar la idempotencia")
    lote = []
    for fila in filas:
        faltantes = [c for c in columnas if c not in fila]
        if faltantes:
            raise ValueError(f"Faltan columnas {faltantes} en la fila {fila}")
        normalizada = {c: (None if fila[c] is None else str(fila[c])) for c in columnas}
        normalizada["_LOTE_ID"] = lote_id
        normalizada["_CARGADO_EN"] = cargado_en
        lote.append(normalizada)
    return lote


def a_json_lines(filas: list[dict]) -> str:
    return "\n".join(json.dumps(f, ensure_ascii=False, sort_keys=True) for f in filas)


def desde_json_lines(texto: str) -> list[dict]:
    return [json.loads(linea) for linea in texto.splitlines() if linea.strip()]
