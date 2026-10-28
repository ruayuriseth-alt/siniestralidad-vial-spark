"""Descarga el conector de Kafka sin pedirle a Spark que copie los JAR con winutils."""

import shutil
import urllib.request
from pathlib import Path

from src.common.paths import RAIZ

DESTINO = RAIZ / "lib" / "kafka"
CACHE_IVY = Path.home() / ".ivy2" / "jars"

JARS = (
    (
        "spark-sql-kafka-0-10_2.12-3.5.5.jar",
        "org.apache.spark_spark-sql-kafka-0-10_2.12-3.5.5.jar",
        "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.5/spark-sql-kafka-0-10_2.12-3.5.5.jar",
    ),
    (
        "spark-token-provider-kafka-0-10_2.12-3.5.5.jar",
        "org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.5.5.jar",
        "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.5/spark-token-provider-kafka-0-10_2.12-3.5.5.jar",
    ),
    (
        "kafka-clients-3.4.1.jar",
        "org.apache.kafka_kafka-clients-3.4.1.jar",
        "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar",
    ),
    (
        "lz4-java-1.8.0.jar",
        "org.lz4_lz4-java-1.8.0.jar",
        "https://repo1.maven.org/maven2/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar",
    ),
    (
        "snappy-java-1.1.10.5.jar",
        "org.xerial.snappy_snappy-java-1.1.10.5.jar",
        "https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.10.5/snappy-java-1.1.10.5.jar",
    ),
    (
        "commons-pool2-2.11.1.jar",
        "org.apache.commons_commons-pool2-2.11.1.jar",
        "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar",
    ),
)


def jars_kafka() -> list[str]:
    DESTINO.mkdir(parents=True, exist_ok=True)
    rutas = []
    for nombre, nombre_ivy, url in JARS:
        destino = DESTINO / nombre
        if not destino.exists():
            origen = CACHE_IVY / nombre_ivy
            if origen.exists():
                shutil.copyfile(origen, destino)
            else:
                urllib.request.urlretrieve(url, destino)
        rutas.append(str(destino))
    return rutas
