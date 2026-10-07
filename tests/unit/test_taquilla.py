from datetime import date, datetime
from decimal import Decimal

import pytest

from include.deporte.taquilla import (
    COLUMNAS_RAW,
    MARCA_AGUA_INICIAL,
    consulta_incremental,
    normalizar_filas,
    nueva_marca_agua,
)


def test_consulta_incremental_es_parametrizada_y_acotada():
    sql, params = consulta_incremental(MARCA_AGUA_INICIAL, "2026-10-06 07:00:00")
    assert "updated_at > %s AND updated_at <= %s" in sql
    assert params == (MARCA_AGUA_INICIAL, "2026-10-06 07:00:00")
    assert "2026" not in sql  # los valores nunca se concatenan en el SQL


def test_consulta_rechaza_valores_que_no_son_fechas():
    with pytest.raises(ValueError):
        consulta_incremental("1970-01-01'; DROP TABLE ventas_entradas; --", "2026-10-06 07:00:00")


def test_consulta_rechaza_marca_de_agua_en_el_futuro():
    with pytest.raises(ValueError):
        consulta_incremental("2026-10-07 00:00:00", "2026-10-06 07:00:00")


def test_normalizar_convierte_tipos_de_mysql_a_texto():
    fila = (
        15,
        date(2026, 10, 3),
        "LAN",
        "ORIENTE",
        "WEB",
        120,
        Decimal("50.00"),
        "PAGADA",
        datetime(2026, 9, 28, 10, 0, 0),
        datetime(2026, 9, 28, 10, 0, 0),
    )
    resultado = normalizar_filas([fila])[0]
    assert list(resultado) == COLUMNAS_RAW
    assert resultado["FECHA_PARTIDO"] == "2026-10-03"
    assert resultado["PRECIO_UNITARIO"] == "50.00"
    assert resultado["UPDATED_AT"] == "2026-09-28 10:00:00"


def test_normalizar_detecta_cambios_de_esquema_en_la_fuente():
    with pytest.raises(ValueError, match="columnas"):
        normalizar_filas([(1, 2, 3)])


def test_marca_de_agua_avanza_al_mayor_updated_at():
    filas = [{"UPDATED_AT": "2026-10-01 10:00:00"}, {"UPDATED_AT": "2026-10-02 09:00:00"}]
    assert nueva_marca_agua(filas, "2026-09-30 00:00:00") == "2026-10-02 09:00:00"


def test_marca_de_agua_no_cambia_sin_filas_nuevas():
    assert nueva_marca_agua([], "2026-10-02 09:00:00") == "2026-10-02 09:00:00"


def test_marca_de_agua_nunca_retrocede():
    assert nueva_marca_agua([{"UPDATED_AT": "2026-01-01 00:00:00"}], "2026-10-02 09:00:00") == "2026-10-02 09:00:00"
