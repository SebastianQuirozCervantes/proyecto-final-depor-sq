from datetime import date

import pytest

from include.deporte.carga_fisica import (
    COLUMNAS_ESPERADAS,
    COLUMNAS_RAW,
    a_filas_raw,
    archivos_pendientes,
    evaluar_calidad,
    fecha_desde_nombre_archivo,
    leer_csv,
    validar_carga_fisica,
)
from include.deporte.errores import ErrorCalidadDatos, ErrorEsquemaApi

DIA = date(2026, 10, 5)


def _fila(**cambios):
    fila = {
        "fecha": "2026-10-05",
        "jugador_id": "J010",
        "tipo_sesion": "ENTRENAMIENTO",
        "minutos": "80",
        "distancia_total_m": "5800.5",
        "distancia_alta_intensidad_m": "350.0",
        "sprints": "9",
        "velocidad_max_kmh": "29.4",
        "fc_media_lpm": "152",
        "rpe": "6",
    }
    fila.update(cambios)
    return fila


class TestNombresDeArchivo:
    def test_extrae_la_fecha(self):
        assert fecha_desde_nombre_archivo("carga_fisica_20261005.csv") == DIA

    @pytest.mark.parametrize(
        "nombre", ["carga_fisica_20261005.csv.part", "otro.csv", "carga_fisica_20261399.csv", "carga_fisica_2026.csv"]
    )
    def test_ignora_nombres_que_no_son_entregas(self, nombre):
        assert fecha_desde_nombre_archivo(nombre) is None

    def test_pendientes_ordenados_sin_procesados_ni_futuros(self):
        disponibles = [
            "carga_fisica_20261005.csv",
            "carga_fisica_20261003.csv",
            "carga_fisica_20261004.csv",
            "carga_fisica_20261006.csv",  # posterior a la fecha lógica
            "carga_fisica_20261002.csv.part",  # aún se está escribiendo
        ]
        procesados = {"carga_fisica_20261004.csv"}
        assert archivos_pendientes(disponibles, procesados, DIA, limite=10) == [
            "carga_fisica_20261003.csv",
            "carga_fisica_20261005.csv",
        ]

    def test_pendientes_respeta_el_limite_empezando_por_los_mas_antiguos(self):
        disponibles = [f"carga_fisica_202610{d:02d}.csv" for d in range(1, 6)]
        assert archivos_pendientes(disponibles, set(), DIA, limite=2) == [
            "carga_fisica_20261001.csv",
            "carga_fisica_20261002.csv",
        ]


class TestValidacion:
    def test_fila_correcta_es_valida(self):
        validas, rechazadas = validar_carga_fisica([_fila()], DIA)
        assert len(validas) == 1 and rechazadas == []

    @pytest.mark.parametrize(
        "cambios, motivo",
        [
            ({"rpe": "11"}, "rpe fuera de rango"),
            ({"rpe": "0"}, "rpe fuera de rango"),
            ({"distancia_total_m": "-5800"}, "distancia_total_m fuera de rango"),
            ({"jugador_id": "  "}, "jugador_id vacío"),
            ({"velocidad_max_kmh": "N/D"}, "velocidad_max_kmh no numérico"),
            ({"tipo_sesion": "AMISTOSO"}, "tipo_sesion inválido"),
            ({"fecha": "2026-10-04"}, "no coincide"),
            ({"distancia_total_m": "2000", "distancia_alta_intensidad_m": "3000"}, "mayor que la distancia total"),
        ],
    )
    def test_detecta_errores_tipicos_del_proveedor(self, cambios, motivo):
        validas, rechazadas = validar_carga_fisica([_fila(**cambios)], DIA)
        assert validas == []
        assert motivo in rechazadas[0]["motivo_rechazo"]
        assert rechazadas[0]["linea"] == 2

    def test_rechaza_duplicados_del_mismo_jugador_y_sesion(self):
        validas, rechazadas = validar_carga_fisica([_fila(), _fila(minutos="81")], DIA)
        assert len(validas) == 1
        assert "duplicada" in rechazadas[0]["motivo_rechazo"]

    def test_leer_csv_exige_las_columnas_del_contrato(self):
        with pytest.raises(ErrorEsquemaApi, match="rpe"):
            leer_csv("fecha,jugador_id\n2026-10-05,J001\n")

    def test_leer_csv_y_convertir_a_raw(self):
        contenido = ",".join(COLUMNAS_ESPERADAS) + "\n" + ",".join(_fila().values()) + "\n"
        validas, _ = validar_carga_fisica(leer_csv(contenido), DIA)
        raw = a_filas_raw(validas, "carga_fisica_20261005.csv")
        assert list(raw[0]) == COLUMNAS_RAW
        assert raw[0]["ARCHIVO_ORIGEN"] == "carga_fisica_20261005.csv"


class TestCalidad:
    def test_pocas_filas_malas_se_toleran(self):
        assert evaluar_calidad(98, 2, umbral=0.05, archivo="a.csv") == pytest.approx(0.02)

    def test_demasiadas_filas_malas_detienen_la_carga(self):
        with pytest.raises(ErrorCalidadDatos, match="30.0%"):
            evaluar_calidad(70, 30, umbral=0.05, archivo="a.csv")

    def test_archivo_chico_tolera_una_fila_mala(self):
        # Día de partido: 14 filas, 1 mala = 7 % pero no debe bloquear el archivo.
        assert evaluar_calidad(13, 1, umbral=0.05, archivo="a.csv") == pytest.approx(1 / 14)

    def test_archivo_chico_con_varias_filas_malas_falla(self):
        with pytest.raises(ErrorCalidadDatos):
            evaluar_calidad(11, 3, umbral=0.05, archivo="a.csv")

    def test_archivo_vacio_es_un_error(self):
        with pytest.raises(ErrorCalidadDatos, match="no tiene filas"):
            evaluar_calidad(0, 0, umbral=0.05, archivo="a.csv")
