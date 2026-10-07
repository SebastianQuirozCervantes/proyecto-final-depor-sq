"""Transformaciones de la fuente 2: archivos GPS que llegan por SFTP."""

from __future__ import annotations

import csv
import io
import re
from datetime import date, datetime

from include.deporte.errores import ErrorCalidadDatos, ErrorEsquemaApi

PATRON_ARCHIVO = re.compile(r"^carga_fisica_(\d{8})\.csv$")

COLUMNAS_ESPERADAS = [
    "fecha",
    "jugador_id",
    "tipo_sesion",
    "minutos",
    "distancia_total_m",
    "distancia_alta_intensidad_m",
    "sprints",
    "velocidad_max_kmh",
    "fc_media_lpm",
    "rpe",
]

TIPOS_SESION = {"ENTRENAMIENTO", "PARTIDO", "RECUPERACION"}

# Rangos fisiológicamente plausibles: lo que caiga fuera es un error de
# exportación del proveedor, no un dato real.
RANGOS = {
    "minutos": (0, 150),
    "distancia_total_m": (0, 16000),
    "distancia_alta_intensidad_m": (0, 4000),
    "sprints": (0, 120),
    "velocidad_max_kmh": (0, 40),
    "fc_media_lpm": (40, 220),
    "rpe": (1, 10),
}

COLUMNAS_RAW = [c.upper() for c in COLUMNAS_ESPERADAS] + ["ARCHIVO_ORIGEN"]


def nombre_archivo_para(dia: date) -> str:
    return f"carga_fisica_{dia:%Y%m%d}.csv"


def fecha_desde_nombre_archivo(nombre: str) -> date | None:
    """Extrae la fecha de 'carga_fisica_YYYYMMDD.csv' (None si no aplica)."""
    coincidencia = PATRON_ARCHIVO.match(nombre)
    if not coincidencia:
        return None
    try:
        return datetime.strptime(coincidencia.group(1), "%Y%m%d").date()
    except ValueError:
        return None


def archivos_pendientes(disponibles: list[str], procesados: set[str], hasta: date, limite: int) -> list[str]:
    """Archivos del SFTP que todavía no se cargaron, del más antiguo al más nuevo.

    * Ignora archivos con otro nombre (temporales ``.part``, basura, etc.).
    * Ignora archivos con fecha posterior a la fecha lógica de la corrida.
    * Respeta un límite por corrida para no saturar el worker en un backfill.
    """
    candidatos = []
    for nombre in disponibles:
        fecha = fecha_desde_nombre_archivo(nombre)
        if fecha is None or fecha > hasta or nombre in procesados:
            continue
        candidatos.append((fecha, nombre))
    return [nombre for _, nombre in sorted(candidatos)][:limite]


def leer_csv(contenido: str) -> list[dict]:
    lector = csv.DictReader(io.StringIO(contenido))
    columnas = lector.fieldnames or []
    faltantes = [c for c in COLUMNAS_ESPERADAS if c not in columnas]
    if faltantes:
        raise ErrorEsquemaApi(f"El archivo GPS no tiene las columnas obligatorias: {faltantes}")
    return list(lector)


def _validar_fila(fila: dict, fecha_archivo: date) -> str | None:
    """Devuelve el motivo de rechazo o None si la fila es válida."""
    if not (fila.get("jugador_id") or "").strip():
        return "jugador_id vacío"
    if fila.get("tipo_sesion") not in TIPOS_SESION:
        return f"tipo_sesion inválido: {fila.get('tipo_sesion')!r}"
    try:
        if date.fromisoformat(fila["fecha"]) != fecha_archivo:
            return "la fecha de la fila no coincide con la del archivo"
    except (TypeError, ValueError):
        return f"fecha inválida: {fila.get('fecha')!r}"
    for campo, (minimo, maximo) in RANGOS.items():
        try:
            valor = float(fila[campo])
        except (TypeError, ValueError):
            return f"{campo} no numérico: {fila.get(campo)!r}"
        if not minimo <= valor <= maximo:
            return f"{campo} fuera de rango [{minimo}, {maximo}]: {valor}"
    if float(fila["distancia_alta_intensidad_m"]) > float(fila["distancia_total_m"]):
        return "distancia de alta intensidad mayor que la distancia total"
    return None


def validar_carga_fisica(filas: list[dict], fecha_archivo: date) -> tuple[list[dict], list[dict]]:
    """Separa filas válidas de rechazadas (las rechazadas llevan el motivo)."""
    validas, rechazadas = [], []
    vistos: set[tuple[str, str]] = set()
    for numero, fila in enumerate(filas, start=2):  # línea 1 = cabecera
        motivo = _validar_fila(fila, fecha_archivo)
        clave = ((fila.get("jugador_id") or "").strip(), fila.get("tipo_sesion") or "")
        if motivo is None and clave in vistos:
            motivo = "fila duplicada (mismo jugador y sesión)"
        if motivo:
            rechazadas.append({**fila, "linea": numero, "motivo_rechazo": motivo})
        else:
            vistos.add(clave)
            validas.append({c: fila[c].strip() if isinstance(fila[c], str) else fila[c] for c in COLUMNAS_ESPERADAS})
    return validas, rechazadas


def evaluar_calidad(validas: int, rechazadas: int, umbral: float, archivo: str, minimo_tolerado: int = 2) -> float:
    """Falla con un error claro si el archivo es demasiado malo para cargarlo.

    Un 1-2 % de filas malas es normal y se manda a cuarentena; un 30 % indica
    que el proveedor cambió el formato y cargar el archivo contaminaría los
    indicadores de riesgo de lesión. Como los archivos de día de partido son
    pequeños (~14 filas), se toleran siempre hasta ``minimo_tolerado`` filas
    malas: una sola fila sucia no debe bloquear el archivo completo.
    """
    total = validas + rechazadas
    if total == 0:
        raise ErrorCalidadDatos(f"El archivo {archivo} no tiene filas.")
    proporcion = rechazadas / total
    if rechazadas > max(minimo_tolerado, umbral * total):
        raise ErrorCalidadDatos(
            f"{archivo}: {rechazadas}/{total} filas inválidas ({proporcion:.1%}) supera el umbral de {umbral:.0%}."
        )
    return proporcion


def a_filas_raw(validas: list[dict], archivo: str) -> list[dict]:
    return [{**{k.upper(): v for k, v in fila.items()}, "ARCHIVO_ORIGEN": archivo} for fila in validas]
