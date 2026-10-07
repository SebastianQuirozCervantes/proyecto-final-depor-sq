"""Transformaciones de la fuente 1: API de partidos (football-data.org v4)."""

from __future__ import annotations

from datetime import date, timedelta

from include.deporte.errores import ErrorEsquemaApi

# Orden y nombre de columnas de la tabla RAW.API_PARTIDOS en Snowflake.
COLUMNAS_PARTIDOS = [
    "MATCH_ID",
    "COMPETICION",
    "TEMPORADA_INICIO",
    "JORNADA",
    "FECHA_UTC",
    "ESTADO",
    "LOCAL_ID",
    "LOCAL_NOMBRE",
    "VISITANTE_ID",
    "VISITANTE_NOMBRE",
    "GOLES_LOCAL",
    "GOLES_VISITANTE",
    "GANADOR",
    "ULTIMA_ACTUALIZACION",
    "EXTRAIDO_EN",
]


def dividir_rango_fechas(desde: date, hasta: date, max_dias: int = 10) -> list[tuple[date, date]]:
    """Parte [desde, hasta] en tramos que respetan el rango máximo de la API.

    football-data.org rechaza (HTTP 400) rangos de más de 10 días, así que una
    ventana de 6 semanas se pide en varios tramos contiguos y sin solapamiento.
    """
    if hasta < desde:
        raise ValueError(f"Rango inválido: {desde} > {hasta}")
    if max_dias < 1:
        raise ValueError("max_dias debe ser >= 1")
    tramos = []
    inicio = desde
    while inicio <= hasta:
        fin = min(inicio + timedelta(days=max_dias), hasta)
        tramos.append((inicio, fin))
        inicio = fin + timedelta(days=1)
    return tramos


def ventana_extraccion(fecha_logica: date, dias: int) -> tuple[date, date]:
    """Ventana de re-extracción: los últimos ``dias`` hasta la fecha lógica.

    Se re-extrae una ventana (y no solo el día) porque los marcadores pueden
    corregirse después del partido y porque los partidos aplazados cambian de
    estado. dbt se queda luego con la versión más reciente de cada partido.
    """
    return fecha_logica - timedelta(days=dias - 1), fecha_logica


def _requerido(diccionario: dict, *ruta: str):
    actual = diccionario
    for clave in ruta:
        if not isinstance(actual, dict) or clave not in actual:
            raise ErrorEsquemaApi(f"Falta el campo {'.'.join(ruta)} en la respuesta de la API")
        actual = actual[clave]
    return actual


def aplanar_partidos(payload: dict, competicion: str, extraido_en: str) -> list[dict]:
    """Convierte la respuesta JSON anidada de la API en filas planas para RAW."""
    if not isinstance(payload, dict) or not isinstance(payload.get("matches"), list):
        raise ErrorEsquemaApi("La respuesta de la API no contiene la lista 'matches'")

    filas = []
    for partido in payload["matches"]:
        tiempo_completo = _requerido(partido, "score", "fullTime")
        temporada = partido.get("season") or {}
        filas.append(
            {
                "MATCH_ID": int(_requerido(partido, "id")),
                "COMPETICION": competicion,
                "TEMPORADA_INICIO": temporada.get("startDate"),
                "JORNADA": partido.get("matchday"),
                "FECHA_UTC": _requerido(partido, "utcDate"),
                "ESTADO": _requerido(partido, "status"),
                "LOCAL_ID": int(_requerido(partido, "homeTeam", "id")),
                "LOCAL_NOMBRE": _requerido(partido, "homeTeam", "name"),
                "VISITANTE_ID": int(_requerido(partido, "awayTeam", "id")),
                "VISITANTE_NOMBRE": _requerido(partido, "awayTeam", "name"),
                "GOLES_LOCAL": tiempo_completo.get("home"),
                "GOLES_VISITANTE": tiempo_completo.get("away"),
                "GANADOR": partido["score"].get("winner"),
                "ULTIMA_ACTUALIZACION": partido.get("lastUpdated"),
                "EXTRAIDO_EN": extraido_en,
            }
        )
    return filas


def deduplicar_partidos(filas: list[dict]) -> list[dict]:
    """Si un partido aparece en dos tramos, se queda la versión más reciente."""
    por_id: dict[int, dict] = {}
    for fila in filas:
        previa = por_id.get(fila["MATCH_ID"])
        if previa is None or (fila.get("ULTIMA_ACTUALIZACION") or "") >= (previa.get("ULTIMA_ACTUALIZACION") or ""):
            por_id[fila["MATCH_ID"]] = fila
    return sorted(por_id.values(), key=lambda f: (f["FECHA_UTC"], f["MATCH_ID"]))
