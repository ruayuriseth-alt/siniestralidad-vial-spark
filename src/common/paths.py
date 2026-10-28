"""Rutas estables del proyecto, independientes del directorio de trabajo."""

from pathlib import Path

RAIZ = Path(__file__).resolve().parents[2]
RUTA_DATOS = RAIZ / "data"
RUTA_RAW = (
    RUTA_DATOS
    / "raw"
    / "VEHICULOS_INVOLUCRADOS_EN_UN_ACCIDENTE_DE_TRANSITO_LEY_2251-2022.csv"
)
RUTA_PROCESADOS = RUTA_DATOS / "processed"
RUTA_REPORTES = RAIZ / "output" / "reports"
RUTA_GRAFICOS = RAIZ / "output" / "charts"
RUTA_STREAMING = RAIZ / "output" / "streaming"

NOMBRE_PARQUET = "vehiculos_limpios.parquet"
RUTA_PARQUET = RUTA_PROCESADOS / NOMBRE_PARQUET
