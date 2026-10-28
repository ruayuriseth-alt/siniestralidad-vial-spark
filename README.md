# Análisis de la siniestralidad vial en Colombia con Apache Spark

Procesamiento por lotes de los vehículos involucrados en siniestros viales con muerte o lesión, publicados por el RUNT en Datos Abiertos Colombia. El foco territorial es Colombia y, dentro del archivo, el departamento del Cesar y el municipio de Valledupar.

La simulación con Kafka reproduce registros históricos. No es un flujo de accidentes en vivo. `received_at` es la hora de envío y `fecha_accidente` sigue siendo el mes del siniestro.

## Problema

La pregunta de trabajo es: qué patrones temporales, territoriales y vehiculares se observan en los registros, y cómo se comparan Colombia, Cesar y Valledupar.

Objetivos de esta fase:

1. Describir la distribución por departamento, municipio, tipo de vehículo y gravedad.
2. Reconstruir la evolución mensual a partir de fechas `MM/AAAA`.
3. Clasificar la antigüedad solo cuando la edad del vehículo es utilizable.
4. Comparar el país con Cesar y Valledupar.
5. Distinguir filas publicadas de combinaciones distintas, porque la fuente no identifica siniestros ni vehículos.

Cada fila es un vehículo involucrado. No es un siniestro único y el resultado no es una tasa de riesgo.

## Fuente

Consulta del 26 de octubre de 2026.

- Portal: https://www.datos.gov.co/Transporte/VEHICULOS-INVOLUCRADOS-EN-UN-ACCIDENTE-DE-TRANSITO/6jmc-vaxk
- Publica: Ministerio de Transporte, RUNT 2.0, en cumplimiento de la Ley 2251 de 2022
- Licencia: Creative Commons Attribution-ShareAlike 4.0
- Archivo: CSV, UTF-8, 42.642.682 bytes
- Última modificación publicada: 4 de septiembre de 2026
- Descarga directa: https://www.datos.gov.co/api/views/6jmc-vaxk/rows.csv?accessType=DOWNLOAD

Guardar el archivo, sin modificarlo, en:

`data/raw/VEHICULOS_INVOLUCRADOS_EN_UN_ACCIDENTE_DE_TRANSITO_LEY_2251-2022.csv`

El perfil medido sobre ese archivo: 450.335 filas, 9 columnas, de enero de 2022 a septiembre de 2026. Cesar tiene 1.985 filas y Valledupar 486. El detalle de cobertura, calidad y limitaciones está en [docs/definicion_problema.md](docs/definicion_problema.md).

Columnas publicadas:

`MARCA_VEHICULO`, `MODELO_VEHICULO`, `TIPO_VEHICULO`, `EDAD_VEHICULO`, `FECHA_ACCIDENTE`, `GRAVEDAD_ACCIDENTE`, `DEPARTAMENTO_ACCIDENTE`, `MUNICIPIO_ACCIDENTE`, `AUTORIDAD_DE_TRANSITO`.

## Requisitos

Verificado en este equipo: Python 3.12.2, Java 22.0.2 y PySpark 3.5.5. Java 22 arranca una sesión local. PySpark advierte que no encuentra `winutils.exe`. La sesión compila `src/winfs/WinLocalFileSystem.java` para poder guardar Parquet en Windows sin ese programa. Las transformaciones `map` y `reduceByKey` no llegan a completarse: el proceso worker de Python cierra el socket sin devolver filas. La guía del curso admite RDDs o DataFrames, y esta fase usa DataFrames.

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

Las variables opcionales están en `.env.example`. No hay credenciales.

## Ejecutar el batch

Desde la raíz del proyecto:

```powershell
python -m src.batch.run_batch
```

Pruebas, con un CSV sintético separado del archivo oficial:

```powershell
python -m pytest tests -q
```

## Kafka y streaming

Hace falta Docker. El compose levanta un Kafka local en el puerto 9092, sin credenciales.

```powershell
docker compose up -d
python -m src.streaming.event_generator --cantidad 50 --intervalo 0.2
python -m src.streaming.kafka_consumer --una-pasada
```

El generador, por defecto, envía una fila por combinación distinta, ordenada por el mes histórico. Una muestra corta sale de los primeros meses de 2022. `--todas-las-filas` reenvía también las copias repetidas. `--intervalo 0` las manda seguidas.

El consumidor calcula el conteo por gravedad, una ventana de 10 segundos por tipo de vehículo y el conteo de mensajes que no cumplen el JSON. La ventana usa `received_at`. Para verlo de forma continua, en otra terminal:

```powershell
python -m src.streaming.kafka_consumer
```

Esa ejecución sigue hasta Ctrl+C. Cada arranque con el mismo checkpoint no vuelve a leer los mensajes ya procesados. Para repetir la lectura hay que borrar `output/streaming/checkpoints/`.

La primera ejecución del consumidor copia el conector Kafka a `lib/kafka/`. No se usa `spark.jars.packages`, porque en Windows esa vía llama a `winutils`. El productor pide `acks=all` y espera la respuesta de este bróker antes de contar el mensaje como enviado. No se midió latencia ni el comportamiento si el bróker se cae.

Los resultados de la pasada quedan en `output/streaming/gravedad/`, `output/streaming/ventana_tipo/` y `output/streaming/invalidos/`.

En la prueba de este equipo, con el bróker en el puerto 9092, se confirmaron 20 mensajes válidos y se agregaron 2 JSON inválidos. El consumidor contó 20 registros `CON HERIDOS`. La ventana de recepción, de 00:56:40 a 00:56:50 UTC, agrupó motocicleta (12), automóvil (4), bus (2), camión (1) y camioneta (1). Esa ventana sale de `received_at`. Los registros históricos de esa muestra son de `01/2022`.

## Resultados

El comando regenera:

- `data/processed/vehiculos_limpios.parquet`
- `output/reports/resumen_ejecucion.md` y las tablas CSV
- `output/charts/*.png`

Las tablas traen dos medidas: `filas` y `combinaciones`. La segunda cuenta una vez cada combinación idéntica de las nueve columnas. Esas repeticiones no se eliminan, porque sin una llave no se puede demostrar que sean el mismo vehículo.

Los gráficos de volumen de Cesar y Valledupar van aparte del total nacional: 450.335 filas y 486 filas no se leen en el mismo eje.

## Decisiones de limpieza

- Se conservan todas las filas.
- La fecha se interpreta como mes. El orden temporal usa la fecha derivada, no el texto `MM/AAAA`, porque `12/2025` queda después de `09/2026` en orden alfabético.
- La edad vacía queda `SIN DATO`. La edad mayor de 100 queda `EDAD NO UTILIZABLE` y sale de las categorías de antigüedad. No se imputa.
- Valledupar se filtra con `DEPARTAMENTO_ACCIDENTE = CESAR` y `MUNICIPIO_ACCIDENTE = VALLEDUPAR`. `SAN MARTIN` también existe en Meta.
- El texto original del municipio se conserva. Si trae ` - `, se separa un complemento en columnas derivadas.

## Limitaciones

El conjunto cabe en un equipo personal: 40,7 MiB no son Big Data a gran escala. Spark se usa para practicar el procesamiento distribuido que pide el curso. El archivo solo cubre vehículos en siniestros con muerte o lesión registrados por el RUNT. No trae coordenadas, hora, placa ni identificador del siniestro. Septiembre de 2026 tiene muy pocos registros y el archivo se actualizó el 4 de septiembre de 2026.
