"""Mock local de la API de football-data.org (v4).

Imita el contrato real de ``GET /v4/competitions/{code}/matches`` para que el
DAG de ingesta se escriba exactamente igual que si consumiera la API real
(lo permite la sección 12 del enunciado: "simular la fuente con datos
realistas, siempre que el código de ingesta esté escrito como si fuera a
conectarse a la fuente real").

Comportamientos realistas que reproduce:
* Autenticación por cabecera ``X-Auth-Token`` (403 si falta o es inválida).
* Límite de tasa: N peticiones por minuto (429 + ``Retry-After``).
* Rango máximo de fechas de 10 días por petición (400 si se excede).

Solo usa la librería estándar de Python, para no necesitar imagen propia.
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time
from datetime import UTC, date, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import calendario as cal  # noqa: E402

TOKEN = os.environ.get("MOCK_API_TOKEN", "")
LIMITE_POR_MINUTO = int(os.environ.get("MOCK_API_LIMITE_POR_MINUTO", "10"))
MAX_DIAS_RANGO = 10
NOMBRES = {"LAN": "Liga Andina", "CPA": "Copa Andina"}

_lock = threading.Lock()
_peticiones: list[float] = []


def _estado(partido: cal.Partido, ahora: datetime) -> str:
    if ahora >= partido.inicio_utc + timedelta(hours=2):
        return "FINISHED"
    if ahora >= partido.inicio_utc:
        return "IN_PLAY"
    return "TIMED"


def _inicio_temporada(partido: cal.Partido) -> date:
    if partido.competicion == "LAN":
        return cal.ANCLA_LIGA + timedelta(weeks=cal.JORNADAS_POR_TEMPORADA * (partido.temporada - 1))
    return date(partido.fecha.year, 1, 1)


def serializar(partido: cal.Partido, ahora: datetime) -> dict:
    estado = _estado(partido, ahora)
    goles_local = goles_visita = None
    ganador = None
    if estado == "FINISHED":
        goles_local, goles_visita = cal.marcador(partido)
        if goles_local > goles_visita:
            ganador = "HOME_TEAM"
        elif goles_local < goles_visita:
            ganador = "AWAY_TEAM"
        else:
            ganador = "DRAW"
    ultima_actualizacion = min(ahora, partido.inicio_utc + timedelta(hours=2, minutes=15))
    return {
        "id": partido.match_id,
        "utcDate": partido.inicio_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "status": estado,
        "matchday": partido.jornada,
        "stage": "REGULAR_SEASON" if partido.competicion == "LAN" else "GROUP_STAGE",
        "season": {
            "id": partido.temporada,
            "startDate": _inicio_temporada(partido).isoformat(),
        },
        "homeTeam": partido.local,
        "awayTeam": partido.visitante,
        "score": {
            "winner": ganador,
            "duration": "REGULAR",
            "fullTime": {"home": goles_local, "away": goles_visita},
        },
        "lastUpdated": ultima_actualizacion.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


class Handler(BaseHTTPRequestHandler):
    server_version = "MockFootballData/1.0"

    def _json(self, status: int, cuerpo: dict, cabeceras: dict | None = None) -> None:
        datos = json.dumps(cuerpo, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(datos)))
        for clave, valor in (cabeceras or {}).items():
            self.send_header(clave, valor)
        self.end_headers()
        self.wfile.write(datos)

    def _limite_excedido(self) -> int | None:
        """Devuelve los segundos a esperar si se superó el límite de tasa."""
        ahora = time.time()
        with _lock:
            while _peticiones and ahora - _peticiones[0] > 60:
                _peticiones.pop(0)
            if len(_peticiones) >= LIMITE_POR_MINUTO:
                return int(60 - (ahora - _peticiones[0])) + 1
            _peticiones.append(ahora)
        return None

    def do_GET(self) -> None:  # noqa: N802 (nombre impuesto por http.server)
        url = urlparse(self.path)
        if url.path == "/health":
            self._json(200, {"status": "ok"})
            return

        if not TOKEN or self.headers.get("X-Auth-Token") != TOKEN:
            self._json(403, {"message": "The resource you are looking for is restricted.", "errorCode": 403})
            return

        espera = self._limite_excedido()
        if espera is not None:
            self._json(
                429,
                {"message": f"You reached your request limit. Wait {espera} seconds.", "errorCode": 429},
                {"Retry-After": str(espera), "X-RequestCounter-Reset": str(espera)},
            )
            return

        partes = url.path.strip("/").split("/")
        if len(partes) != 4 or partes[:2] != ["v4", "competitions"] or partes[3] != "matches":
            self._json(404, {"message": "Not found", "errorCode": 404})
            return

        codigo = partes[2].upper()
        if codigo not in NOMBRES:
            self._json(404, {"message": f"Competition {codigo} not found", "errorCode": 404})
            return

        params = parse_qs(url.query)
        try:
            desde = date.fromisoformat(params["dateFrom"][0])
            hasta = date.fromisoformat(params["dateTo"][0])
        except (KeyError, ValueError):
            self._json(400, {"message": "dateFrom and dateTo (YYYY-MM-DD) are required", "errorCode": 400})
            return
        if hasta < desde or (hasta - desde).days > MAX_DIAS_RANGO:
            self._json(400, {"message": "The given date range is too large (max 10 days).", "errorCode": 400})
            return

        ahora = datetime.now(UTC)
        partidos = [serializar(p, ahora) for p in cal.partidos_entre(codigo, desde, hasta)]
        self._json(
            200,
            {
                "filters": {"dateFrom": desde.isoformat(), "dateTo": hasta.isoformat()},
                "resultSet": {"count": len(partidos)},
                "competition": {"code": codigo, "name": NOMBRES[codigo]},
                "matches": partidos,
            },
        )

    def log_message(self, formato: str, *args: object) -> None:
        sys.stdout.write("[mock-api] " + (formato % args) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    puerto = int(os.environ.get("MOCK_API_PORT", "8000"))
    print(f"[mock-api] escuchando en :{puerto} (límite {LIMITE_POR_MINUTO} req/min)", flush=True)
    ThreadingHTTPServer(("0.0.0.0", puerto), Handler).serve_forever()
