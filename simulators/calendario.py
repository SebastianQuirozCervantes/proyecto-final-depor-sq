"""Calendario deportivo compartido por los simuladores de fuentes.

Este módulo SOLO lo usan los simuladores (mock de la API de partidos y el
generador de archivos GPS). Define de forma determinística:

* La Liga Andina (código ``LAN``): 10 equipos, todos contra todos ida y vuelta
  (18 jornadas por temporada), una jornada cada sábado desde el 2026-08-01.
  El calendario es "perpetuo": al terminar una temporada empieza la siguiente,
  así el proyecto sigue generando datos sin importar la fecha en que se ejecute.
* La Copa Andina (código ``CPA``): el Club Deportivo SQ juega un partido
  de copa cada dos miércoles desde el 2026-08-12.

Regla clave que comparte con la base MySQL de taquilla (infra/mysql/init.sql):
el Club Deportivo SQ juega de LOCAL en las jornadas de liga impares.
"""

from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

ANCLA_LIGA = date(2026, 8, 1)  # sábado, jornada 1 de la temporada 1
ANCLA_COPA = date(2026, 8, 12)  # miércoles, ronda 1 de la copa
JORNADAS_POR_TEMPORADA = 18
HORA_INICIO_UTC = 20  # los partidos empiezan a las 20:00 UTC

CLUB_ID = 100

EQUIPOS: list[dict] = [
    {"id": 100, "name": "Club Deportivo SQ", "shortName": "SQ", "tla": "CDA"},
    {"id": 101, "name": "Atlético Pacífico", "shortName": "Pacífico", "tla": "ATP"},
    {"id": 102, "name": "Real Cordillera", "shortName": "Cordillera", "tla": "RCO"},
    {"id": 103, "name": "Sporting Huascarán", "shortName": "Huascarán", "tla": "SHU"},
    {"id": 104, "name": "Unión Altiplano", "shortName": "Altiplano", "tla": "UAL"},
    {"id": 105, "name": "Deportivo Amazonas", "shortName": "Amazonas", "tla": "DAM"},
    {"id": 106, "name": "Racing Misti", "shortName": "Misti", "tla": "RMI"},
    {"id": 107, "name": "Juventud Costera", "shortName": "Costera", "tla": "JCO"},
    {"id": 108, "name": "Estrella del Sur", "shortName": "Estrella", "tla": "EDS"},
    {"id": 109, "name": "Club Titicaca", "shortName": "Titicaca", "tla": "CTI"},
]

# Plantel del Club Deportivo SQ. Debe coincidir con el seed de dbt
# dbt/deporte/seeds/jugadores.csv (catálogo maestro del club).
PLANTEL: list[dict] = [
    {"jugador_id": "J001", "posicion": "POR"},
    {"jugador_id": "J002", "posicion": "POR"},
    {"jugador_id": "J003", "posicion": "DEF"},
    {"jugador_id": "J004", "posicion": "DEF"},
    {"jugador_id": "J005", "posicion": "DEF"},
    {"jugador_id": "J006", "posicion": "DEF"},
    {"jugador_id": "J007", "posicion": "DEF"},
    {"jugador_id": "J008", "posicion": "DEF"},
    {"jugador_id": "J009", "posicion": "DEF"},
    {"jugador_id": "J010", "posicion": "MED"},
    {"jugador_id": "J011", "posicion": "MED"},
    {"jugador_id": "J012", "posicion": "MED"},
    {"jugador_id": "J013", "posicion": "MED"},
    {"jugador_id": "J014", "posicion": "MED"},
    {"jugador_id": "J015", "posicion": "MED"},
    {"jugador_id": "J016", "posicion": "MED"},
    {"jugador_id": "J017", "posicion": "DEL"},
    {"jugador_id": "J018", "posicion": "DEL"},
    {"jugador_id": "J019", "posicion": "DEL"},
    {"jugador_id": "J020", "posicion": "DEL"},
    {"jugador_id": "J021", "posicion": "DEL"},
    {"jugador_id": "J022", "posicion": "DEL"},
]


@dataclass(frozen=True)
class Partido:
    match_id: int
    competicion: str
    fecha: date
    jornada: int  # jornada (liga) o ronda (copa) dentro de la temporada
    temporada: int
    local: dict
    visitante: dict

    @property
    def inicio_utc(self) -> datetime:
        return datetime(self.fecha.year, self.fecha.month, self.fecha.day, HORA_INICIO_UTC, tzinfo=UTC)

    def involucra_club(self) -> bool:
        return CLUB_ID in (self.local["id"], self.visitante["id"])


def rng_para(*partes: object) -> random.Random:
    """Generador pseudoaleatorio determinístico a partir de una semilla textual."""
    semilla = hashlib.sha256("|".join(map(str, partes)).encode()).hexdigest()
    return random.Random(int(semilla[:16], 16))


def _emparejamientos_ronda(ronda: int) -> list[tuple[dict, dict]]:
    """Método del círculo: devuelve los 5 cruces de la ronda (0..8)."""
    fijo, resto = EQUIPOS[0], EQUIPOS[1:]
    rotados = resto[ronda:] + resto[:ronda]
    orden = [fijo] + rotados
    n = len(orden)
    return [(orden[i], orden[n - 1 - i]) for i in range(n // 2)]


def partidos_liga_de_jornada(jornada_global: int) -> list[Partido]:
    """Partidos de la jornada ``jornada_global`` (1, 2, 3, ... sin fin)."""
    temporada = (jornada_global - 1) // JORNADAS_POR_TEMPORADA + 1
    jornada = (jornada_global - 1) % JORNADAS_POR_TEMPORADA + 1
    ronda = (jornada - 1) % 9
    segunda_vuelta = jornada > 9
    fecha = ANCLA_LIGA + timedelta(days=7 * (jornada_global - 1))

    partidos = []
    for idx, (a, b) in enumerate(_emparejamientos_ronda(ronda)):
        local, visita = (b, a) if segunda_vuelta else (a, b)
        if CLUB_ID in (a["id"], b["id"]):
            club = a if a["id"] == CLUB_ID else b
            rival = b if club is a else a
            # Regla compartida con la taquilla: local en jornadas impares.
            local, visita = (club, rival) if jornada_global % 2 == 1 else (rival, club)
        match_id = 1_000_000 + jornada_global * 10 + idx
        partidos.append(Partido(match_id, "LAN", fecha, jornada, temporada, local, visita))
    return partidos


def partido_copa_de_ronda(ronda_global: int) -> Partido:
    fecha = ANCLA_COPA + timedelta(days=14 * (ronda_global - 1))
    rivales = EQUIPOS[1:]
    rival = rivales[(ronda_global - 1) % len(rivales)]
    club = EQUIPOS[0]
    local, visita = (club, rival) if ronda_global % 2 == 1 else (rival, club)
    temporada = (fecha.year - ANCLA_COPA.year) + 1
    return Partido(2_000_000 + ronda_global, "CPA", fecha, ronda_global, temporada, local, visita)


def partidos_entre(competicion: str, desde: date, hasta: date) -> list[Partido]:
    """Todos los partidos de una competición con fecha en [desde, hasta]."""
    resultado: list[Partido] = []
    if competicion == "LAN":
        jornada = max(1, (desde - ANCLA_LIGA).days // 7 + 1)
        while True:
            fecha = ANCLA_LIGA + timedelta(days=7 * (jornada - 1))
            if fecha > hasta:
                break
            if fecha >= desde:
                resultado.extend(partidos_liga_de_jornada(jornada))
            jornada += 1
    elif competicion == "CPA":
        ronda = max(1, (desde - ANCLA_COPA).days // 14 + 1)
        while True:
            partido = partido_copa_de_ronda(ronda)
            if partido.fecha > hasta:
                break
            if partido.fecha >= desde:
                resultado.append(partido)
            ronda += 1
    else:
        raise KeyError(competicion)
    return resultado


def partido_del_club_en(fecha: date) -> Partido | None:
    for competicion in ("LAN", "CPA"):
        for partido in partidos_entre(competicion, fecha, fecha):
            if partido.involucra_club():
                return partido
    return None


def marcador(partido: Partido) -> tuple[int, int]:
    """Goles (local, visitante) determinísticos para un partido."""
    rng = rng_para("marcador", partido.match_id)
    pesos = [0.26, 0.34, 0.23, 0.11, 0.06]  # probabilidad de 0..4 goles
    goles_local = rng.choices(range(5), weights=pesos)[0]
    goles_visita = rng.choices(range(5), weights=pesos)[0]
    # Pequeña ventaja deportiva del Club Deportivo SQ.
    if partido.local["id"] == CLUB_ID and rng.random() < 0.25:
        goles_local += 1
    if partido.visitante["id"] == CLUB_ID and rng.random() < 0.15:
        goles_visita += 1
    return goles_local, goles_visita
