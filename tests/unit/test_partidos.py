from datetime import date

import pytest

from include.deporte.errores import ErrorEsquemaApi
from include.deporte.partidos import (
    COLUMNAS_PARTIDOS,
    aplanar_partidos,
    deduplicar_partidos,
    dividir_rango_fechas,
    ventana_extraccion,
)


def _partido(match_id=1, status="FINISHED", home=2, away=1, actualizado="2026-10-03T22:15:00Z"):
    return {
        "id": match_id,
        "utcDate": "2026-10-03T20:00:00Z",
        "status": status,
        "matchday": 10,
        "season": {"id": 1, "startDate": "2026-08-01"},
        "homeTeam": {"id": 100, "name": "Club Deportivo SQ"},
        "awayTeam": {"id": 101, "name": "Atlético Pacífico"},
        "score": {"winner": "HOME_TEAM", "fullTime": {"home": home, "away": away}},
        "lastUpdated": actualizado,
    }


class TestDividirRangoFechas:
    def test_ventana_de_seis_semanas_respeta_el_maximo_de_la_api(self):
        tramos = dividir_rango_fechas(date(2026, 8, 25), date(2026, 10, 5), max_dias=10)
        assert all((fin - inicio).days <= 10 for inicio, fin in tramos)

    def test_tramos_contiguos_sin_huecos_ni_solapamientos(self):
        tramos = dividir_rango_fechas(date(2026, 8, 25), date(2026, 10, 5))
        assert tramos[0][0] == date(2026, 8, 25)
        assert tramos[-1][1] == date(2026, 10, 5)
        for (_, fin), (inicio_siguiente, _) in zip(tramos, tramos[1:], strict=False):
            assert (inicio_siguiente - fin).days == 1

    def test_un_solo_dia(self):
        assert dividir_rango_fechas(date(2026, 10, 5), date(2026, 10, 5)) == [(date(2026, 10, 5), date(2026, 10, 5))]

    def test_rango_invertido_falla(self):
        with pytest.raises(ValueError):
            dividir_rango_fechas(date(2026, 10, 5), date(2026, 10, 1))


def test_ventana_extraccion_incluye_la_fecha_logica():
    desde, hasta = ventana_extraccion(date(2026, 10, 5), dias=7)
    assert (desde, hasta) == (date(2026, 9, 29), date(2026, 10, 5))


class TestAplanarPartidos:
    def test_genera_todas_las_columnas_raw(self):
        filas = aplanar_partidos({"matches": [_partido()]}, "LAN", "2026-10-06T07:00:00Z")
        assert list(filas[0]) == COLUMNAS_PARTIDOS
        assert filas[0]["GOLES_LOCAL"] == 2
        assert filas[0]["COMPETICION"] == "LAN"

    def test_partido_no_jugado_tiene_marcador_nulo(self):
        partido = _partido(status="TIMED", home=None, away=None)
        partido["score"]["winner"] = None
        fila = aplanar_partidos({"matches": [partido]}, "LAN", "x")[0]
        assert fila["GOLES_LOCAL"] is None and fila["GANADOR"] is None

    def test_respuesta_sin_matches_rompe_el_contrato(self):
        with pytest.raises(ErrorEsquemaApi):
            aplanar_partidos({"message": "error"}, "LAN", "x")

    def test_partido_sin_equipo_local_rompe_el_contrato(self):
        partido = _partido()
        del partido["homeTeam"]
        with pytest.raises(ErrorEsquemaApi, match="homeTeam"):
            aplanar_partidos({"matches": [partido]}, "LAN", "x")


def test_deduplicar_se_queda_con_la_version_mas_reciente():
    vieja = aplanar_partidos({"matches": [_partido(home=1, actualizado="2026-10-03T22:00:00Z")]}, "LAN", "x")[0]
    corregida = aplanar_partidos({"matches": [_partido(home=2, actualizado="2026-10-04T09:00:00Z")]}, "LAN", "x")[0]
    resultado = deduplicar_partidos([corregida, vieja])
    assert len(resultado) == 1
    assert resultado[0]["GOLES_LOCAL"] == 2
