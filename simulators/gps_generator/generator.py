"""Simulador del proveedor de chalecos GPS del departamento de rendimiento.

Cada día el proveedor deja en el servidor SFTP del club un archivo
``carga_fisica_YYYYMMDD.csv`` con la carga física de cada jugador en la sesión
de ese día (entrenamiento, partido o recuperación). Este script reproduce ese
comportamiento escribiendo en el volumen que el contenedor SFTP expone.

Para que el caso sea "del mundo real", aproximadamente el 1,5 % de las filas
llega con errores típicos de exportación (RPE fuera de escala, distancias
negativas, jugador vacío o valores "N/D"). El pipeline debe detectarlas y
mandarlas a cuarentena en lugar de fallar en silencio.
"""

from __future__ import annotations

import csv
import os
import sys
import time
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import calendario as cal  # noqa: E402

DIRECTORIO_SALIDA = os.environ.get("GPS_OUTPUT_DIR", "/data/gps")
DIAS_HISTORIA = int(os.environ.get("GPS_DIAS_HISTORIA", "42"))
INTERVALO_SEGUNDOS = int(os.environ.get("GPS_INTERVALO_SEGUNDOS", "300"))
PORCENTAJE_FILAS_SUCIAS = 0.015

COLUMNAS = [
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


def _dias_hasta_proximo_partido(dia: date) -> int | None:
    for delta in range(0, 8):
        if cal.partido_del_club_en(dia + timedelta(days=delta)):
            return delta
    return None


def _fila(dia: date, jugador: dict, tipo: str, minutos: int, intensidad: float, rng) -> dict:
    metros_por_minuto = {"PARTIDO": 112, "ENTRENAMIENTO": 72, "RECUPERACION": 45}[tipo]
    if jugador["posicion"] == "POR":
        metros_por_minuto *= 0.55
    distancia = minutos * metros_por_minuto * intensidad * rng.uniform(0.9, 1.1)
    porcentaje_ai = {"PARTIDO": 0.09, "ENTRENAMIENTO": 0.06, "RECUPERACION": 0.01}[tipo]
    distancia_ai = distancia * porcentaje_ai * rng.uniform(0.7, 1.3)
    rpe_base = {"PARTIDO": 8, "ENTRENAMIENTO": 6, "RECUPERACION": 3}[tipo]
    rpe = max(1, min(10, round(rpe_base * intensidad + rng.uniform(-1.2, 1.2))))
    return {
        "fecha": dia.isoformat(),
        "jugador_id": jugador["jugador_id"],
        "tipo_sesion": tipo,
        "minutos": minutos,
        "distancia_total_m": round(distancia, 1),
        "distancia_alta_intensidad_m": round(distancia_ai, 1),
        "sprints": max(0, round(distancia_ai / 38 + rng.uniform(-2, 2))),
        "velocidad_max_kmh": round(rng.uniform(24, 34) if tipo != "RECUPERACION" else rng.uniform(15, 21), 1),
        "fc_media_lpm": round(rng.uniform(120, 145) + 25 * intensidad * (rpe / 10)),
        "rpe": rpe,
    }


def _ensuciar(fila: dict, rng) -> dict:
    """Introduce un error típico de exportación del proveedor."""
    error = rng.choice(["rpe", "distancia", "jugador", "velocidad"])
    if error == "rpe":
        fila["rpe"] = rng.choice([0, 11, 15])
    elif error == "distancia":
        fila["distancia_total_m"] = -abs(fila["distancia_total_m"])
    elif error == "jugador":
        fila["jugador_id"] = ""
    else:
        fila["velocidad_max_kmh"] = "N/D"
    return fila


def filas_del_dia(dia: date) -> list[dict]:
    rng = cal.rng_para("gps", dia.isoformat())
    lesionados = set(
        rng_lesion.choice(cal.PLANTEL)["jugador_id"]
        for rng_lesion in [cal.rng_para("lesion", dia.isocalendar()[1], i) for i in range(2)]
    )
    disponibles = [j for j in cal.PLANTEL if j["jugador_id"] not in lesionados]

    partido_hoy = cal.partido_del_club_en(dia)
    partido_ayer = cal.partido_del_club_en(dia - timedelta(days=1))
    filas: list[dict] = []

    if partido_hoy:
        por_posicion = {pos: [j for j in disponibles if j["posicion"] == pos] for pos in ("POR", "DEF", "MED", "DEL")}
        titulares = (
            rng.sample(por_posicion["POR"], 1)
            + rng.sample(por_posicion["DEF"], 4)
            + rng.sample(por_posicion["MED"], 4)
            + rng.sample(por_posicion["DEL"], 2)
        )
        suplentes = rng.sample([j for j in disponibles if j not in titulares and j["posicion"] != "POR"], 3)
        for jugador in titulares:
            minutos = 90 if jugador["posicion"] == "POR" else rng.choice([90, 90, 90, 75, 68, 60])
            filas.append(_fila(dia, jugador, "PARTIDO", minutos, rng.uniform(0.95, 1.1), rng))
        for jugador in suplentes:
            filas.append(_fila(dia, jugador, "PARTIDO", rng.randint(12, 32), rng.uniform(1.0, 1.15), rng))
    elif partido_ayer:
        for jugador in disponibles:
            filas.append(_fila(dia, jugador, "RECUPERACION", rng.randint(30, 45), rng.uniform(0.5, 0.7), rng))
    else:
        dias = _dias_hasta_proximo_partido(dia)
        intensidad = {1: 0.6, 2: 0.85}.get(dias, 1.0)
        for jugador in disponibles:
            filas.append(
                _fila(dia, jugador, "ENTRENAMIENTO", rng.randint(60, 100), intensidad * rng.uniform(0.9, 1.1), rng)
            )

    for fila in filas:
        if rng.random() < PORCENTAJE_FILAS_SUCIAS:
            _ensuciar(fila, rng)
    return filas


def escribir_archivo(dia: date) -> str | None:
    ruta = os.path.join(DIRECTORIO_SALIDA, f"carga_fisica_{dia:%Y%m%d}.csv")
    if os.path.exists(ruta):
        return None
    temporal = ruta + ".part"
    with open(temporal, "w", newline="", encoding="utf-8") as archivo:
        escritor = csv.DictWriter(archivo, fieldnames=COLUMNAS)
        escritor.writeheader()
        escritor.writerows(filas_del_dia(dia))
    # Renombrado atómico: el sensor nunca ve un archivo a medio escribir.
    os.replace(temporal, ruta)
    return ruta


def ciclo() -> None:
    os.makedirs(DIRECTORIO_SALIDA, exist_ok=True)
    hoy = date.today()
    # La sesión de un día se entrega cuando ese día terminó: hasta ayer.
    for delta in range(DIAS_HISTORIA, 0, -1):
        ruta = escribir_archivo(hoy - timedelta(days=delta))
        if ruta:
            print(f"[gps-generator] archivo entregado: {ruta}", flush=True)


if __name__ == "__main__":
    print(f"[gps-generator] escribiendo en {DIRECTORIO_SALIDA} ({DIAS_HISTORIA} días de historia)", flush=True)
    while True:
        ciclo()
        if os.environ.get("GPS_UNA_VEZ") == "1":
            break
        time.sleep(INTERVALO_SEGUNDOS)
