"""Publica registros históricos en Kafka. No es una fuente de accidentes en vivo."""

import argparse
import os
import sys
import time
import uuid

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
from pyspark.sql import functions as F

from src.batch.clean_data import LLAVES_COMBINACION
from src.common.paths import RUTA_PARQUET
from src.common.spark_session import crear_spark
from src.streaming.eventos import AVISO, TOPIC, construir_evento, marca_de_tiempo, serializar_evento


def leer_muestra(cantidad: int, todas_las_filas: bool) -> list[dict]:
    if not RUTA_PARQUET.exists():
        raise FileNotFoundError(
            "No está el Parquet procesado. Ejecute primero python -m src.batch.run_batch."
        )
    spark = crear_spark("generador-siniestros")
    try:
        datos = spark.read.parquet("file:///" + RUTA_PARQUET.resolve().as_posix())
        if not todas_las_filas:
            datos = datos.dropDuplicates(list(LLAVES_COMBINACION))
        filas = (
            datos.orderBy(
                F.col("fecha_mes").asc_nulls_last(),
                "departamento_accidente",
                "municipio_accidente",
            )
            .limit(cantidad)
            .collect()
        )
        return [fila.asDict() for fila in filas]
    finally:
        spark.stop()


def publicar(argumentos: argparse.Namespace) -> int:
    print(AVISO)
    modo = "todas las filas" if argumentos.todas_las_filas else "combinaciones distintas"
    print(f"Modo de lectura: {modo}. Cantidad máxima: {argumentos.cantidad}.")
    try:
        registros = leer_muestra(argumentos.cantidad, argumentos.todas_las_filas)
    except FileNotFoundError as exc:
        print(exc)
        return 1
    if not registros:
        print("La muestra quedó vacía.")
        return 1

    try:
        productor = KafkaProducer(
            bootstrap_servers=argumentos.bootstrap.split(","),
            value_serializer=lambda texto: texto.encode("utf-8"),
            key_serializer=lambda texto: texto.encode("utf-8"),
            acks="all",
            retries=3,
            request_timeout_ms=15000,
            api_version_auto_timeout_ms=15000,
        )
    except NoBrokersAvailable:
        print(
            "No hay bróker en "
            f"{argumentos.bootstrap}. Arranque Kafka con: docker compose up -d"
        )
        return 1

    enviados = 0
    try:
        for registro in registros:
            event_id = str(uuid.uuid4())
            evento = construir_evento(registro, event_id, marca_de_tiempo())
            futuro = productor.send(
                argumentos.topic,
                key=event_id,
                value=serializar_evento(evento),
            )
            futuro.get(timeout=15)
            enviados += 1
            print(
                f"Enviado {enviados}: {evento['fecha_accidente']} "
                f"{evento['departamento_accidente']} {evento['tipo_vehiculo']} "
                f"received_at={evento['received_at']}"
            )
            if argumentos.intervalo > 0:
                time.sleep(argumentos.intervalo)
    except KeyboardInterrupt:
        print(f"Envío detenido. Mensajes confirmados por el bróker: {enviados}.")
        return 0
    except (KafkaError, OSError) as exc:
        print(f"Error de envío después de {enviados} mensajes: {exc}")
        return 1
    finally:
        productor.flush()
        productor.close()
    print(f"Envío terminado. Mensajes confirmados por el bróker: {enviados}.")
    return 0


def construir_argumentos(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Reproduce registros históricos de siniestralidad en Kafka."
    )
    parser.add_argument("--cantidad", type=int, default=50)
    parser.add_argument(
        "--intervalo",
        type=float,
        default=0.2,
        help="Segundos entre mensajes. 0 los envía seguidos.",
    )
    parser.add_argument(
        "--bootstrap",
        default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
    )
    parser.add_argument("--topic", default=os.environ.get("KAFKA_TOPIC", TOPIC))
    parser.add_argument(
        "--todas-las-filas",
        action="store_true",
        help="Reenvía filas repetidas. Sin esta opción se envía una fila por combinación.",
    )
    argumentos = parser.parse_args(argv)
    if argumentos.cantidad < 1:
        parser.error("--cantidad debe ser mayor que cero.")
    if argumentos.intervalo < 0:
        parser.error("--intervalo no puede ser negativo.")
    return argumentos


def main(argv: list[str] | None = None) -> int:
    return publicar(construir_argumentos(argv))


if __name__ == "__main__":
    sys.exit(main())
