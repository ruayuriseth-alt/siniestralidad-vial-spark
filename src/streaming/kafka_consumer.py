"""Consume la simulación histórica. Las ventanas usan received_at, no la fecha del siniestro."""

import argparse
import os
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.common.paths import RUTA_STREAMING
from src.common.spark_session import crear_spark
from src.streaming.eventos import AVISO, CAMPOS_REGISTRO, TOPIC
from src.streaming.jars_kafka import jars_kafka
FORMATO_RECEPCION = "yyyy-MM-dd'T'HH:mm:ss'Z'"

TIPOS = {
    "modelo_vehiculo": IntegerType(),
    "edad_vehiculo": IntegerType(),
    "anio": IntegerType(),
    "mes": IntegerType(),
    "multiplicidad": LongType(),
}


def _uri(ruta: Path) -> str:
    return "file:///" + ruta.resolve().as_posix()


def esquema_evento() -> StructType:
    campos = [
        StructField("event_id", StringType()),
        StructField("received_at", StringType()),
        StructField("simulacion", BooleanType()),
        StructField("origen", StringType()),
    ]
    for nombre in CAMPOS_REGISTRO:
        campos.append(StructField(nombre, TIPOS.get(nombre, StringType())))
    campos.append(StructField("_corrupto", StringType()))
    return StructType(campos)


def leer_topic(spark: SparkSession, bootstrap: str, topic: str) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )


def parsear(flujo: DataFrame) -> DataFrame:
    evento = F.from_json(
        F.col("value").cast("string"),
        esquema_evento(),
        {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupto"},
    ).alias("evento")
    return flujo.select(evento).select("evento.*").withColumn(
        "received_ts",
        F.to_timestamp(F.col("received_at"), FORMATO_RECEPCION),
    )


def es_valido() -> F.Column:
    return (
        F.col("_corrupto").isNull()
        & F.col("event_id").isNotNull()
        & (F.col("event_id") != "")
        & F.col("received_ts").isNotNull()
        & F.col("tipo_vehiculo").isNotNull()
        & (F.col("tipo_vehiculo") != "")
        & F.col("gravedad_accidente").isNotNull()
        & (F.col("gravedad_accidente") != "")
        & F.col("fecha_accidente").isNotNull()
        & (F.col("fecha_accidente") != "")
    )


def _consulta(writer, argumentos: argparse.Namespace, nombre: str):
    writer = (
        writer.trigger(availableNow=True)
        if argumentos.una_pasada
        else writer.trigger(processingTime="5 seconds")
    )
    return writer.option(
        "checkpointLocation",
        _uri(RUTA_STREAMING / "checkpoints" / nombre),
    ).queryName(nombre)


def _guardar(lote: DataFrame, _epoca: int, destino: str) -> None:
    if lote.limit(1).count() == 0:
        return
    print(AVISO)
    lote.show(50, truncate=False)
    ruta = RUTA_STREAMING / destino
    lote.coalesce(1).write.mode("overwrite").option("header", True).csv(_uri(ruta))


def iniciar(argumentos: argparse.Namespace) -> int:
    print(AVISO)
    print(
        "La agregación por ventana usa received_at. "
        "fecha_accidente permanece como mes histórico y no define la ventana."
    )
    spark = crear_spark("consumidor-siniestros", classpath_extra=jars_kafka())
    spark.conf.set(
        "spark.sql.streaming.checkpointFileManagerClass",
        "org.apache.spark.sql.execution.streaming.FileSystemBasedCheckpointFileManager",
    )
    try:
        def eventos():
            return parsear(leer_topic(spark, argumentos.bootstrap, argumentos.topic))

        gravedad = (
            eventos()
            .filter(es_valido())
            .groupBy("gravedad_accidente")
            .agg(F.count("*").alias("eventos"))
        )
        ventana = (
            eventos()
            .filter(es_valido())
            .withWatermark("received_ts", "1 minute")
            .groupBy(F.window("received_ts", "10 seconds").alias("ventana"), "tipo_vehiculo")
            .agg(F.count("*").alias("eventos"))
            .select(
                F.col("ventana.start").alias("ventana_inicio"),
                F.col("ventana.end").alias("ventana_fin"),
                "tipo_vehiculo",
                "eventos",
            )
        )
        errores = (
            eventos()
            .filter(~es_valido())
            .groupBy(F.lit("mensaje_invalido").alias("estado"))
            .agg(F.count("*").alias("eventos"))
        )

        consultas = [
            _consulta(
                gravedad.writeStream.outputMode("complete").foreachBatch(
                    lambda lote, epoca: _guardar(lote, epoca, "gravedad")
                ),
                argumentos,
                "conteo-gravedad",
            ).start(),
            _consulta(
                ventana.writeStream.outputMode("update").foreachBatch(
                    lambda lote, epoca: _guardar(lote, epoca, "ventana_tipo")
                ),
                argumentos,
                "ventana-tipo",
            ).start(),
            _consulta(
                errores.writeStream.outputMode("complete").foreachBatch(
                    lambda lote, epoca: _guardar(lote, epoca, "invalidos")
                ),
                argumentos,
                "mensajes-invalidos",
            ).start(),
        ]
        if argumentos.una_pasada:
            for consulta in consultas:
                consulta.awaitTermination()
            return 0
        spark.streams.awaitAnyTermination()
        return 0
    except KeyboardInterrupt:
        print("Consumidor detenido.")
        return 0
    finally:
        spark.stop()


def construir_argumentos(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Consume la reproducción histórica del topic siniestros_viales."
    )
    parser.add_argument(
        "--bootstrap",
        default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
    )
    parser.add_argument("--topic", default=os.environ.get("KAFKA_TOPIC", TOPIC))
    parser.add_argument(
        "--una-pasada",
        action="store_true",
        help="Lee lo que ya está en el topic y termina.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    return iniciar(construir_argumentos(argv))


if __name__ == "__main__":
    sys.exit(main())
