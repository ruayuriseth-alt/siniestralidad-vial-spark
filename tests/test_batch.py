"""Pruebas del procesamiento batch con registros sintéticos, separados del CSV oficial."""

from pathlib import Path

import pytest

from src.batch.charts import barras_vertical
from src.batch.clean_data import calidad_crudo, calidad_limpio, limpiar
from src.batch.exploratory_analysis import (
    comparacion_territorial,
    distribucion_antiguedad,
    evolucion_temporal,
    filas_por_departamento,
)
from src.batch.load_data import leer_vehiculos
from src.common.spark_session import crear_spark

FIXTURA = Path(__file__).parent / "fixtures" / "vehiculos_sinteticos.csv"


@pytest.fixture(scope="module")
def spark():
    sesion = crear_spark("pruebas-siniestralidad")
    yield sesion
    sesion.stop()


@pytest.fixture(scope="module")
def limpio(spark):
    crudo = leer_vehiculos(spark, FIXTURA)
    assert crudo.count() == 14
    assert len(crudo.columns) == 9
    return limpiar(crudo).cache()


def test_fecha_y_departamento_con_coma(limpio):
    san_andres = limpio.filter(
        limpio.departamento_accidente.contains("SAN ANDRES")
    ).collect()
    assert len(san_andres) == 1
    assert san_andres[0]["departamento_accidente"] == (
        "ARCHIPIELAGO DE SAN ANDRES, PROVIDENCIA"
    )
    assert san_andres[0]["anio"] == 2026
    assert san_andres[0]["mes"] == 7
    assert str(san_andres[0]["fecha_mes"]) == "2026-07-01"


def test_conserva_filas_y_marca_repetidas(limpio):
    calidad = calidad_limpio(limpio)
    assert calidad["filas"] == 14
    assert calidad["combinaciones"] == 12
    assert calidad["filas"] - calidad["combinaciones"] == 2
    repetida = limpio.filter(limpio.marca_vehiculo == "AKT").collect()
    assert len(repetida) == 2
    assert {fila["multiplicidad"] for fila in repetida} == {2}
    assert sum(fila["es_primera_combinacion"] for fila in repetida) == 1


def test_edad_y_fecha(limpio):
    por_marca = {fila["marca_vehiculo"]: fila for fila in limpio.collect()}
    assert por_marca["MAZDA"]["edad_estado"] == "SIN DATO"
    assert por_marca["MAZDA"]["categoria_antiguedad"] == "SIN DATO"
    assert por_marca["MAZDA"]["edad_vehiculo"] is None
    assert por_marca["HONDA"]["edad_estado"] == "EDAD NO UTILIZABLE"
    assert por_marca["HONDA"]["categoria_antiguedad"] == "EDAD NO UTILIZABLE"
    assert por_marca["CHEVROLET"]["categoria_antiguedad"] == "21 o mas"
    assert por_marca["TOYOTA"]["edad_estado"] == "EDAD NO NUMERICA"
    assert por_marca["AKT"]["categoria_antiguedad"] == "0-5"
    assert por_marca["BMW"]["fecha_valida"] is False
    assert por_marca["BMW"]["fecha_mes"] is None
    assert por_marca["HINO"]["categoria_antiguedad"] == "11-15"


def test_municipio_compuesto_y_filtro_local(limpio):
    por_marca = {fila["marca_vehiculo"]: fila for fila in limpio.collect()}
    assert por_marca["HINO"]["municipio_base"] == "BOSCONIA"
    assert por_marca["HINO"]["municipio_complemento"] == "ALTO DE MINAS"
    assert por_marca["RENAULT"]["municipio_base_normalizado"] == "AGUSTIN CODAZZI"
    assert por_marca["RENAULT"]["municipio_accidente"] == "AGUSTÍN CODAZZI - CASACAZA"

    territorios = {fila["ambito"]: fila for fila in comparacion_territorial(limpio)}
    assert territorios["Colombia"]["filas"] == 14
    assert territorios["Cesar"]["filas"] == 8
    assert territorios["Valledupar"]["filas"] == 4
    assert territorios["Valledupar"]["combinaciones"] == 3
    assert territorios["Valledupar"]["filas_con_muertos"] == 0
    assert territorios["Cesar"]["filas_con_muertos"] == 2
    meta = limpio.filter(limpio.municipio_accidente == "SAN MARTIN").collect()
    assert len(meta) == 1
    assert meta[0]["departamento_accidente"] == "META"


def test_la_serie_omite_fechas_invalidas_y_el_departamento_suma_las_filas(limpio):
    departamentos = filas_por_departamento(limpio).collect()
    assert sum(fila["filas"] for fila in departamentos) == 14
    meses = {fila["fecha_mes"] for fila in evolucion_temporal(limpio).collect()}
    assert None not in meses
    assert len(meses) == 11


def test_antiguedad_incluye_categorias_no_utilizables(limpio):
    categorias = {
        fila["categoria_antiguedad"]
        for fila in distribucion_antiguedad(limpio).collect()
    }
    assert "SIN DATO" in categorias
    assert "EDAD NO UTILIZABLE" in categorias
    assert "21 o mas" in categorias


def test_calidad_cruda_cuenta_el_vacio_de_edad(spark):
    crudo = leer_vehiculos(spark, FIXTURA)
    calidad = calidad_crudo(crudo)
    assert calidad["filas"] == 14
    assert calidad["vacios"]["EDAD_VEHICULO"] == 1
    assert calidad["combinaciones"] == 12


def test_grafico_crea_archivo(tmp_path):
    destino = tmp_path / "prueba.png"
    barras_vertical(["Cesar", "Valledupar"], [8, 4], destino, "Prueba", "Filas")
    assert destino.is_file()
    assert destino.stat().st_size > 0
