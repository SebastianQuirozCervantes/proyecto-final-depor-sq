import pytest

from include.deporte.config import cargar_configuracion
from include.deporte.errores import ErrorConfiguracion
from include.deporte.lotes import a_json_lines, desde_json_lines, preparar_lote
from include.deporte.reportes import generar_reporte_markdown


class TestConfiguracion:
    def test_sin_variable_usa_valores_por_defecto(self):
        config = cargar_configuracion(None)
        assert config.competiciones == ["LAN", "CPA"]

    def test_permite_sobrescribir_valores(self):
        assert cargar_configuracion({"competiciones": ["PL"], "dias_ventana_api": 10}).dias_ventana_api == 10

    @pytest.mark.parametrize(
        "valor",
        [
            {"competiciones": []},
            {"competiciones": ["XXX"]},
            {"dias_ventana_api": 0},
            {"umbral_filas_invalidas": 0.9},
            {"max_archivos_por_corrida": 0},
            {"clave_inventada": 1},
        ],
    )
    def test_rechaza_configuraciones_invalidas(self, valor):
        with pytest.raises(ErrorConfiguracion):
            cargar_configuracion(valor)


class TestLotes:
    def test_agrega_columnas_de_linaje_y_normaliza_a_texto(self):
        lote = preparar_lote([{"A": 1, "B": None}], ["A", "B"], "lote-1", "2026-10-06T07:00:00")
        assert lote == [{"A": "1", "B": None, "_LOTE_ID": "lote-1", "_CARGADO_EN": "2026-10-06T07:00:00"}]

    def test_exige_lote_id_para_ser_idempotente(self):
        with pytest.raises(ValueError):
            preparar_lote([{"A": 1}], ["A"], "", "x")

    def test_detecta_columnas_faltantes(self):
        with pytest.raises(ValueError, match="Faltan"):
            preparar_lote([{"A": 1}], ["A", "B"], "lote", "x")

    def test_json_lines_ida_y_vuelta(self):
        filas = [{"a": "ñandú", "b": None}, {"a": "2", "b": "3"}]
        assert desde_json_lines(a_json_lines(filas)) == filas


def test_reporte_resume_puntos_y_alertas_de_riesgo():
    partidos = [
        {
            "FECHA_PARTIDO": "2026-10-03",
            "COMPETICION": "LAN",
            "RIVAL": "Estrella del Sur",
            "ES_LOCAL": True,
            "GOLES_FAVOR": 2,
            "GOLES_CONTRA": 1,
            "RESULTADO": "G",
            "PUNTOS": 3,
            "CARGA_EQUIPO_7D_PREVIA": 41000,
            "RECAUDACION_NETA": 152300.0,
        },
        {
            "FECHA_PARTIDO": "2026-09-30",
            "COMPETICION": "CPA",
            "RIVAL": "Racing Misti",
            "ES_LOCAL": False,
            "GOLES_FAVOR": 0,
            "GOLES_CONTRA": 0,
            "RESULTADO": "E",
            "PUNTOS": 1,
            "CARGA_EQUIPO_7D_PREVIA": None,
            "RECAUDACION_NETA": None,
        },
    ]
    riesgo = [
        {"NOMBRE": "Diego Quispe", "POSICION": "DEL", "ACWR": 1.62, "NIVEL_RIESGO": "ALTO"},
        {"NOMBRE": "Luis Mamani", "POSICION": "MED", "ACWR": 1.05, "NIVEL_RIESGO": "OPTIMO"},
    ]
    reporte = generar_reporte_markdown("2026-10-05", partidos, riesgo)
    assert "**4 de 6**" in reporte
    assert "Diego Quispe" in reporte and "1.62" in reporte
    assert "Luis Mamani" not in reporte  # solo se listan jugadores fuera de la zona óptima
    assert "S/ 152,300" in reporte
