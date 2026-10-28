"""Limpieza que conserva el texto original y marca, sin borrar, las filas repetidas."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

LLAVES_COMBINACION = [
    "marca_vehiculo",
    "modelo_texto",
    "tipo_vehiculo",
    "edad_texto",
    "fecha_accidente",
    "gravedad_accidente",
    "departamento_accidente",
    "municipio_accidente",
    "autoridad_de_transito",
]

ORDEN_ANTIGUEDAD = [
    "0-5",
    "6-10",
    "11-15",
    "16-20",
    "21 o mas",
    "SIN DATO",
    "EDAD NO NUMERICA",
    "EDAD NO UTILIZABLE",
]

RENOMBRES = {
    "MARCA_VEHICULO": "marca_vehiculo",
    "MODELO_VEHICULO": "modelo_texto",
    "TIPO_VEHICULO": "tipo_vehiculo",
    "EDAD_VEHICULO": "edad_texto",
    "FECHA_ACCIDENTE": "fecha_accidente",
    "GRAVEDAD_ACCIDENTE": "gravedad_accidente",
    "DEPARTAMENTO_ACCIDENTE": "departamento_accidente",
    "MUNICIPIO_ACCIDENTE": "municipio_accidente",
    "AUTORIDAD_DE_TRANSITO": "autoridad_de_transito",
}


def _sin_tildes(columna: F.Column) -> F.Column:
    return F.translate(columna, "ÁÉÍÓÚÑÜáéíóúñü", "AEIOUNUaeiounu")


def limpiar(df: DataFrame) -> DataFrame:
    """Normaliza tipos y agrega variables derivadas.

    No elimina filas. Una combinación idéntica de las nueve columnas publicadas
    no demuestra que el registro esté duplicado: la fuente no trae identificador
    de vehículo ni de siniestro. La multiplicidad queda marcada para analizar
    filas y combinaciones distintas por separado.
    """
    for origen, destino in RENOMBRES.items():
        df = df.withColumnRenamed(origen, destino)

    for columna in RENOMBRES.values():
        texto = F.trim(F.col(columna))
        df = df.withColumn(
            columna,
            F.when(texto.isNull(), F.lit("")).otherwise(texto),
        )

    modelo_valido = F.col("modelo_texto").rlike(r"^\d+$")
    edad_numerica = F.col("edad_texto").rlike(r"^\d+$")
    mes_texto = F.regexp_extract(
        F.col("fecha_accidente"), r"^(0[1-9]|1[0-2])/([0-9]{4})$", 1
    )
    anio_texto = F.regexp_extract(
        F.col("fecha_accidente"), r"^(0[1-9]|1[0-2])/([0-9]{4})$", 2
    )
    fecha_valida = mes_texto != ""
    partes_municipio = F.split(F.col("municipio_accidente"), r" - ", 2)

    df = (
        df.withColumn("modelo_valido", modelo_valido)
        .withColumn(
            "modelo_vehiculo",
            F.when(modelo_valido, F.col("modelo_texto").cast("int")),
        )
        .withColumn(
            "edad_vehiculo",
            F.when(edad_numerica, F.col("edad_texto").cast("int")),
        )
        .withColumn("fecha_valida", fecha_valida)
        .withColumn("mes", F.when(fecha_valida, mes_texto.cast("int")))
        .withColumn("anio", F.when(fecha_valida, anio_texto.cast("int")))
    )
    df = df.withColumn(
        "fecha_mes",
        F.when(F.col("fecha_valida"), F.make_date(F.col("anio"), F.col("mes"), F.lit(1))),
    )
    df = df.withColumn(
        "edad_estado",
        F.when(
            F.col("edad_texto").isNull() | (F.col("edad_texto") == ""),
            "SIN DATO",
        )
        .when(~edad_numerica, "EDAD NO NUMERICA")
        .when(F.col("edad_vehiculo") > 100, "EDAD NO UTILIZABLE")
        .otherwise("VALIDA"),
    )
    df = df.withColumn(
        "categoria_antiguedad",
        F.when(F.col("edad_estado") != "VALIDA", F.col("edad_estado"))
        .when(F.col("edad_vehiculo") <= 5, "0-5")
        .when(F.col("edad_vehiculo") <= 10, "6-10")
        .when(F.col("edad_vehiculo") <= 15, "11-15")
        .when(F.col("edad_vehiculo") <= 20, "16-20")
        .otherwise("21 o mas"),
    )
    df = (
        df.withColumn("municipio_base", partes_municipio.getItem(0))
        .withColumn(
            "municipio_complemento",
            F.when(F.size(partes_municipio) > 1, partes_municipio.getItem(1)),
        )
        .withColumn("municipio_base_normalizado", _sin_tildes(F.col("municipio_base")))
    )

    ventana = Window.partitionBy(*LLAVES_COMBINACION)
    df = df.withColumn("multiplicidad", F.count(F.lit(1)).over(ventana))
    df = df.withColumn(
        "es_primera_combinacion",
        F.row_number().over(ventana.orderBy(F.lit(1))) == 1,
    )
    return df


def es_cesar() -> F.Column:
    return F.col("departamento_accidente") == "CESAR"


def es_valledupar() -> F.Column:
    return es_cesar() & (F.col("municipio_accidente") == "VALLEDUPAR")


def calidad_crudo(df: DataFrame) -> dict:
    """Resume el archivo tal como se leyó, antes de convertir tipos."""
    df.cache()
    filas = df.count()
    vacios = df.agg(
        *[
            F.sum(
                F.when(
                    F.col(columna).isNull() | (F.trim(F.col(columna)) == ""), 1
                ).otherwise(0)
            ).alias(columna)
            for columna in df.columns
        ]
    ).collect()[0].asDict()
    combinaciones = df.dropDuplicates(df.columns).count()
    return {
        "filas": filas,
        "columnas": len(df.columns),
        "vacios": {clave: int(valor or 0) for clave, valor in vacios.items()},
        "combinaciones": combinaciones,
        "filas_repetidas_adicionales": filas - combinaciones,
    }


def calidad_limpio(df: DataFrame) -> dict:
    """Resume banderas de la copia analítica. El número de filas no cambia."""
    fila = df.agg(
        F.count("*").alias("filas"),
        F.sum(F.when(F.col("es_primera_combinacion"), 1).otherwise(0)).alias(
            "combinaciones"
        ),
        F.sum(F.when(F.col("edad_estado") == "SIN DATO", 1).otherwise(0)).alias(
            "edad_sin_dato"
        ),
        F.sum(F.when(F.col("edad_estado") == "EDAD NO NUMERICA", 1).otherwise(0)).alias(
            "edad_no_numerica"
        ),
        F.sum(
            F.when(F.col("edad_estado") == "EDAD NO UTILIZABLE", 1).otherwise(0)
        ).alias("edad_no_utilizable"),
        F.sum(F.when(~F.col("modelo_valido"), 1).otherwise(0)).alias("modelo_invalido"),
        F.sum(F.when(~F.col("fecha_valida"), 1).otherwise(0)).alias("fecha_invalida"),
        F.sum(F.when(F.col("municipio_complemento").isNotNull(), 1).otherwise(0)).alias(
            "municipio_compuesto"
        ),
        F.sum(F.when(F.col("fecha_accidente") == "07/2024", 1).otherwise(0)).alias(
            "filas_julio_2024"
        ),
        F.sum(
            F.when(
                (F.col("fecha_accidente") == "07/2024")
                & F.col("es_primera_combinacion"),
                1,
            ).otherwise(0)
        ).alias("combinaciones_julio_2024"),
    ).collect()[0].asDict()
    return {clave: int(valor or 0) for clave, valor in fila.items()}
