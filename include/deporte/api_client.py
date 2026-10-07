"""Cliente HTTP para la API de partidos con manejo explícito de errores.

Se inyecta la sesión HTTP (``requests.Session`` en producción, un doble de
prueba en pytest) y la función de espera, para poder probar los reintentos sin
dormir de verdad ni depender de la red.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from datetime import date

from include.deporte.errores import (
    ErrorCredenciales,
    ErrorLimiteTasa,
    ErrorPeticionInvalida,
    ErrorServidorApi,
)

log = logging.getLogger(__name__)


def segundos_de_espera(cabeceras: dict, intento: int, maximo: int = 65) -> int:
    """Cuánto esperar ante un 429.

    Se respeta ``Retry-After`` (o ``X-RequestCounter-Reset``, propio de
    football-data.org) si viene; si no, backoff exponencial 2, 4, 8, 16...
    Siempre acotado por ``maximo`` para no bloquear un worker indefinidamente.
    """
    for clave in ("Retry-After", "X-RequestCounter-Reset"):
        valor = cabeceras.get(clave)
        if valor is not None:
            try:
                return max(1, min(int(float(valor)), maximo))
            except ValueError:
                break
    return min(2 ** (intento + 1), maximo)


class ClienteFootballData:
    def __init__(
        self,
        base_url: str,
        token: str,
        session,
        max_reintentos_429: int = 3,
        timeout: int = 30,
        dormir: Callable[[float], None] = time.sleep,
    ) -> None:
        if not token:
            raise ErrorCredenciales("No hay token configurado en la Connection de la API.")
        self.base_url = base_url.rstrip("/")
        self.session = session
        self.session.headers.update({"X-Auth-Token": token})
        self.max_reintentos_429 = max_reintentos_429
        self.timeout = timeout
        self.dormir = dormir

    def obtener_partidos(self, competicion: str, desde: date, hasta: date) -> dict:
        url = f"{self.base_url}/v4/competitions/{competicion}/matches"
        params = {"dateFrom": desde.isoformat(), "dateTo": hasta.isoformat()}

        for intento in range(self.max_reintentos_429 + 1):
            respuesta = self.session.get(url, params=params, timeout=self.timeout)
            estado = respuesta.status_code

            if estado == 200:
                return respuesta.json()
            if estado in (401, 403):
                raise ErrorCredenciales(f"La API rechazó el token ({estado}). Revisa la Connection 'football_api'.")
            if estado in (400, 404):
                raise ErrorPeticionInvalida(f"Petición inválida ({estado}) a {url} {params}: {respuesta.text[:200]}")
            if estado == 429:
                if intento == self.max_reintentos_429:
                    break
                espera = segundos_de_espera(respuesta.headers, intento)
                log.warning("429 de la API (intento %s). Esperando %ss antes de reintentar.", intento + 1, espera)
                self.dormir(espera)
                continue
            if estado >= 500:
                raise ErrorServidorApi(f"La API respondió {estado}: {respuesta.text[:200]}")
            raise ErrorPeticionInvalida(f"Respuesta inesperada {estado} de la API")

        raise ErrorLimiteTasa(f"Se agotaron {self.max_reintentos_429} reintentos por límite de tasa en {competicion}.")
