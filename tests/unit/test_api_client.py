from datetime import date

import pytest

from include.deporte.api_client import ClienteFootballData, segundos_de_espera
from include.deporte.errores import (
    ErrorCredenciales,
    ErrorLimiteTasa,
    ErrorPeticionInvalida,
    ErrorReintentable,
    ErrorServidorApi,
)


class RespuestaFalsa:
    def __init__(self, status_code, cuerpo=None, headers=None):
        self.status_code = status_code
        self._cuerpo = cuerpo or {}
        self.headers = headers or {}
        self.text = str(self._cuerpo)

    def json(self):
        return self._cuerpo


class SesionFalsa:
    """Doble de requests.Session que devuelve respuestas en orden."""

    def __init__(self, respuestas):
        self.respuestas = list(respuestas)
        self.headers = {}
        self.llamadas = []

    def get(self, url, params=None, timeout=None):
        self.llamadas.append((url, params))
        return self.respuestas.pop(0)


def _cliente(respuestas, esperas=None):
    esperas = esperas if esperas is not None else []
    sesion = SesionFalsa(respuestas)
    cliente = ClienteFootballData("http://api/", "secreto", sesion, max_reintentos_429=2, dormir=esperas.append)
    return cliente, sesion


def test_envia_token_por_cabecera_y_construye_la_url():
    cliente, sesion = _cliente([RespuestaFalsa(200, {"matches": []})])
    cliente.obtener_partidos("LAN", date(2026, 10, 1), date(2026, 10, 5))
    assert sesion.headers["X-Auth-Token"] == "secreto"
    assert sesion.llamadas == [
        ("http://api/v4/competitions/LAN/matches", {"dateFrom": "2026-10-01", "dateTo": "2026-10-05"})
    ]


def test_reintenta_ante_429_respetando_retry_after():
    esperas = []
    cliente, _ = _cliente(
        [RespuestaFalsa(429, headers={"Retry-After": "7"}), RespuestaFalsa(200, {"matches": [1]})], esperas
    )
    assert cliente.obtener_partidos("LAN", date(2026, 10, 1), date(2026, 10, 2)) == {"matches": [1]}
    assert esperas == [7]


def test_agotar_reintentos_429_es_un_error_reintentable_para_airflow():
    cliente, _ = _cliente([RespuestaFalsa(429)] * 3)
    with pytest.raises(ErrorLimiteTasa) as error:
        cliente.obtener_partidos("LAN", date(2026, 10, 1), date(2026, 10, 2))
    assert isinstance(error.value, ErrorReintentable)


@pytest.mark.parametrize("codigo", [401, 403])
def test_token_invalido_falla_sin_reintentar(codigo):
    cliente, sesion = _cliente([RespuestaFalsa(codigo)])
    with pytest.raises(ErrorCredenciales):
        cliente.obtener_partidos("LAN", date(2026, 10, 1), date(2026, 10, 2))
    assert len(sesion.llamadas) == 1


def test_rango_invalido_es_error_definitivo():
    cliente, _ = _cliente([RespuestaFalsa(400, {"message": "range too large"})])
    with pytest.raises(ErrorPeticionInvalida):
        cliente.obtener_partidos("LAN", date(2026, 9, 1), date(2026, 10, 2))


def test_error_5xx_es_reintentable():
    cliente, _ = _cliente([RespuestaFalsa(503)])
    with pytest.raises(ErrorServidorApi):
        cliente.obtener_partidos("LAN", date(2026, 10, 1), date(2026, 10, 2))


def test_sin_token_no_se_construye_el_cliente():
    with pytest.raises(ErrorCredenciales):
        ClienteFootballData("http://api", "", SesionFalsa([]))


@pytest.mark.parametrize(
    "cabeceras, intento, esperado",
    [
        ({"Retry-After": "30"}, 0, 30),
        ({"X-RequestCounter-Reset": "12"}, 0, 12),
        ({"Retry-After": "999"}, 0, 65),  # acotado al máximo
        ({}, 0, 2),  # backoff exponencial 2, 4, 8...
        ({}, 2, 8),
        ({"Retry-After": "no-numero"}, 1, 4),
    ],
)
def test_segundos_de_espera(cabeceras, intento, esperado):
    assert segundos_de_espera(cabeceras, intento) == esperado
