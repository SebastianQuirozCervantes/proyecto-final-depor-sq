import pytest

from include.deporte.warehouse import cargar_lote_idempotente


class CursorFalso:
    def __init__(self, registro):
        self.registro = registro

    def execute(self, sql, params=None):
        self.registro.append((sql, params))

    def close(self):
        pass


class ConexionFalsa:
    def __init__(self):
        self.sentencias = []

    def cursor(self):
        return CursorFalso(self.sentencias)


def test_borra_el_lote_antes_de_insertar():
    conexion, escritos = ConexionFalsa(), []
    total = cargar_lote_idempotente(
        conexion, "API_PARTIDOS", "api_2026-10-05", [{"A": "1"}], lambda c, f, t: escritos.append((t, f))
    )
    assert total == 1
    assert conexion.sentencias == [("DELETE FROM RAW.API_PARTIDOS WHERE _LOTE_ID = %s", ("api_2026-10-05",))]
    assert escritos == [("API_PARTIDOS", [{"A": "1"}])]


def test_lote_vacio_solo_limpia():
    conexion, escritos = ConexionFalsa(), []
    assert cargar_lote_idempotente(conexion, "GPS_CARGA_FISICA", "x", [], lambda *a: escritos.append(a)) == 0
    assert escritos == []
    assert len(conexion.sentencias) == 1


def test_rechaza_tablas_fuera_de_la_lista_blanca():
    with pytest.raises(ValueError):
        cargar_lote_idempotente(ConexionFalsa(), "API_PARTIDOS; DROP TABLE X", "x", [], lambda *a: None)
