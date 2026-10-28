"""Lectura del CSV oficial, sin modificar el archivo de origen."""

import csv
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

COLUMNAS_ORIGEN = [
    "MARCA_VEHICULO",
    "MODELO_VEHICULO",
    "TIPO_VEHICULO",
    "EDAD_VEHICULO",
    "FECHA_ACCIDENTE",
    "GRAVEDAD_ACCIDENTE",
    "DEPARTAMENTO_ACCIDENTE",
    "MUNICIPIO_ACCIDENTE",
    "AUTORIDAD_DE_TRANSITO",
]

ESQUEMA = StructType(
    [StructField(nombre, StringType(), True) for nombre in COLUMNAS_ORIGEN]
)

URL_DESCARGA = (
    "https://www.datos.gov.co/api/views/6jmc-vaxk/rows.csv?accessType=DOWNLOAD"
)


def leer_vehiculos(spark: SparkSession, ruta: Path) -> DataFrame:
    """Lee el CSV como texto y exige el encabezado publicado por el portal.

    Los vacíos se conservan como cadena vacía para contarlos de forma explícita.
    El esquema es posicional y coincide con el archivo verificado.
    """
    if not ruta.is_file():
        raise FileNotFoundError(
            "No está el CSV original en "
            f"{ruta}. Descárgalo desde {URL_DESCARGA} "
            "y guárdalo en data/raw/ con el nombre del archivo publicado."
        )

    with ruta.open("r", encoding="utf-8-sig", newline="") as archivo:
        encabezado = next(csv.reader(archivo))
    if encabezado != COLUMNAS_ORIGEN:
        raise ValueError(
            "El encabezado del CSV no coincide con el esquema verificado. "
            f"Encabezado leído: {encabezado}"
        )

    return (
        spark.read.option("header", True)
        .option("encoding", "UTF-8")
        .option("sep", ",")
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "PERMISSIVE")
        .option("nullValue", "__NULL_AUSENTE__")
        .option("emptyValue", "")
        .schema(ESQUEMA)
        .csv(ruta.resolve().as_posix())
    )
