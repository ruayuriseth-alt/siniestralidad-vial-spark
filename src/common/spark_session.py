"""Sesión local de Spark para el procesamiento por lotes."""

import os
import subprocess
import sys
from pathlib import Path

from pyspark.sql import SparkSession

from src.common.paths import RAIZ


def _habilitar_escritura_windows() -> str | None:
    """Compila un sistema de archivos local que no llama a winutils.

    Hadoop, al crear carpetas en Windows, ejecuta `winutils.exe chmod`.
    Esta máquina no tiene ese programa. La clase evita esa llamada.
    """
    if os.name != "nt":
        return None
    origen = RAIZ / "src" / "winfs" / "WinLocalFileSystem.java"
    destino = RAIZ / "build" / "classes"
    clase = destino / "winfs" / "WinLocalFileSystem.class"
    if not clase.exists() or clase.stat().st_mtime < origen.stat().st_mtime:
        destino.mkdir(parents=True, exist_ok=True)
        import pyspark

        jar = next((Path(pyspark.__file__).parent / "jars").glob("hadoop-client-api-*.jar"))
        compilacion = subprocess.run(
            ["javac", "--release", "17", "-cp", str(jar), "-d", str(destino), str(origen)],
            capture_output=True,
            text=True,
            check=False,
        )
        if compilacion.returncode != 0:
            raise RuntimeError(
                "No se pudo compilar el sistema de archivos local para Windows.\n"
                + compilacion.stdout
                + compilacion.stderr
            )
    return str(destino)


def crear_spark(
    nombre: str = "siniestralidad-vial",
    classpath_extra: list[str] | None = None,
) -> SparkSession:
    """Crea una sesión local y fija el intérprete de Python de este entorno."""
    ejecutable = sys.executable
    os.environ["PYSPARK_PYTHON"] = ejecutable
    os.environ["PYSPARK_DRIVER_PYTHON"] = ejecutable
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    constructor = (
        SparkSession.builder.appName(nombre)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.pyspark.python", ejecutable)
        .config("spark.pyspark.driver.python", ejecutable)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
    )
    clases = _habilitar_escritura_windows()
    partes = []
    if clases:
        partes.append(clases)
        constructor = constructor.config(
            "spark.hadoop.fs.file.impl", "winfs.WinLocalFileSystem"
        )
    if classpath_extra:
        partes.extend(classpath_extra)
    if partes:
        ruta = os.pathsep.join(partes)
        constructor = constructor.config("spark.driver.extraClassPath", ruta).config(
            "spark.executor.extraClassPath", ruta
        )
    spark = constructor.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
