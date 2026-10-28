"""Métricas exploratorias. La unidad de todas las cifras es el vehículo registrado."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.batch.clean_data import ORDEN_ANTIGUEDAD, es_cesar, es_valledupar


def _combinaciones(condicion: F.Column | None = None) -> F.Column:
    criterio = F.col("es_primera_combinacion")
    if condicion is not None:
        criterio = criterio & condicion
    return F.sum(F.when(criterio, 1).otherwise(0))


def filas_por_departamento(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("departamento_accidente")
        .agg(
            F.count("*").alias("filas"),
            _combinaciones().alias("combinaciones"),
        )
        .orderBy(F.desc("filas"), "departamento_accidente")
    )


def filas_por_municipio(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("departamento_accidente", "municipio_accidente")
        .agg(
            F.count("*").alias("filas"),
            _combinaciones().alias("combinaciones"),
        )
        .orderBy(F.desc("filas"), "departamento_accidente", "municipio_accidente")
    )


def distribucion_tipo(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("tipo_vehiculo")
        .agg(F.count("*").alias("filas"), _combinaciones().alias("combinaciones"))
        .orderBy(F.desc("filas"), "tipo_vehiculo")
    )


def distribucion_gravedad(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("gravedad_accidente")
        .agg(F.count("*").alias("filas"), _combinaciones().alias("combinaciones"))
        .orderBy(F.desc("filas"))
    )


def distribucion_antiguedad(df: DataFrame) -> DataFrame:
    orden = F.create_map(
        *[
            elemento
            for posicion, categoria in enumerate(ORDEN_ANTIGUEDAD)
            for elemento in (F.lit(categoria), F.lit(posicion))
        ]
    )
    return (
        df.groupBy("categoria_antiguedad")
        .agg(F.count("*").alias("filas"), _combinaciones().alias("combinaciones"))
        .orderBy(orden[F.col("categoria_antiguedad")])
    )


def evolucion_temporal(df: DataFrame) -> DataFrame:
    """Serie mensual. Las fechas que no tienen forma MM/AAAA quedan fuera de esta serie."""
    return (
        df.filter(F.col("fecha_valida"))
        .groupBy("anio", "mes", "fecha_mes")
        .agg(
            F.count("*").alias("filas"),
            _combinaciones().alias("combinaciones"),
            F.sum(F.when(es_cesar(), 1).otherwise(0)).alias("filas_cesar"),
            _combinaciones(es_cesar()).alias("combinaciones_cesar"),
            F.sum(F.when(es_valledupar(), 1).otherwise(0)).alias("filas_valledupar"),
            _combinaciones(es_valledupar()).alias("combinaciones_valledupar"),
        )
        .orderBy("fecha_mes")
    )


def metricas_ambito(df: DataFrame, ambito: str) -> dict:
    fila = df.agg(
        F.count("*").alias("filas"),
        _combinaciones().alias("combinaciones"),
        F.sum(F.when(F.col("gravedad_accidente") == "CON HERIDOS", 1).otherwise(0)).alias(
            "filas_con_heridos"
        ),
        F.sum(F.when(F.col("gravedad_accidente") == "CON MUERTOS", 1).otherwise(0)).alias(
            "filas_con_muertos"
        ),
        _combinaciones(F.col("gravedad_accidente") == "CON HERIDOS").alias(
            "combinaciones_con_heridos"
        ),
        _combinaciones(F.col("gravedad_accidente") == "CON MUERTOS").alias(
            "combinaciones_con_muertos"
        ),
        F.sum(F.when(F.col("tipo_vehiculo") == "MOTOCICLETA", 1).otherwise(0)).alias(
            "filas_motocicleta"
        ),
    ).collect()[0].asDict()
    resultado = {clave: int(valor or 0) for clave, valor in fila.items()}
    resultado["ambito"] = ambito
    return resultado


def comparacion_territorial(df: DataFrame) -> list[dict]:
    """Colombia, Cesar y Valledupar. Valledupar es subconjunto de Cesar."""
    cesar = df.filter(es_cesar())
    valledupar = cesar.filter(F.col("municipio_accidente") == "VALLEDUPAR")
    return [
        metricas_ambito(df, "Colombia"),
        metricas_ambito(cesar, "Cesar"),
        metricas_ambito(valledupar, "Valledupar"),
    ]


def categorias_frecuentes(df: DataFrame, columna: str, limite: int = 15) -> DataFrame:
    return (
        df.groupBy(columna)
        .agg(F.count("*").alias("filas"), _combinaciones().alias("combinaciones"))
        .orderBy(F.desc("filas"), columna)
        .limit(limite)
    )


def municipios_compuestos(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("municipio_complemento").isNotNull())
        .groupBy(
            "departamento_accidente",
            "municipio_accidente",
            "municipio_base",
            "municipio_complemento",
            "municipio_base_normalizado",
        )
        .agg(F.count("*").alias("filas"))
        .orderBy(F.desc("filas"), "municipio_accidente")
    )


def combinacion_mas_repetida(df: DataFrame) -> dict:
    fila = (
        df.groupBy(
            "marca_vehiculo",
            "modelo_texto",
            "tipo_vehiculo",
            "edad_texto",
            "fecha_accidente",
            "gravedad_accidente",
            "departamento_accidente",
            "municipio_accidente",
            "autoridad_de_transito",
        )
        .agg(F.count("*").alias("filas"))
        .orderBy(F.desc("filas"))
        .limit(1)
        .collect()[0]
        .asDict()
    )
    fila["filas"] = int(fila["filas"])
    return fila
