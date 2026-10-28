# Definición del problema y conjunto de datos

Consulta de la fuente: 26 de octubre de 2026.

## Problema

El proyecto se llama **Análisis de la siniestralidad vial en Colombia mediante Apache Spark y procesamiento de eventos con Kafka**.

El problema analítico es describir patrones temporales, territoriales y vehiculares en los registros públicos de vehículos involucrados en siniestros con muerte o lesión. El resultado informa la descripción y la prevención. No establece causas.

Hay dos niveles:

- Nacional: todos los registros publicados.
- Local: departamento del Cesar y municipio de Valledupar, con los conteos verificados en la fuente.

Pregunta: ¿qué patrones temporales, geográficos y relacionados con los vehículos se pueden identificar, y qué características tienen los registros de Valledupar y del Cesar?

Objetivos:

1. Identificar la distribución por departamento, municipio, tipo de vehículo y gravedad.
2. Analizar la evolución según la precisión real de las fechas, que es mensual.
3. Examinar la antigüedad del vehículo frente al tipo y la gravedad solo cuando la edad sea utilizable.
4. Comparar Colombia, Cesar y Valledupar.
5. Procesar por lotes con PySpark y, en la fase siguiente, simular eventos con Kafka y Spark Structured Streaming.

## Justificación

La seguridad vial es un problema de salud pública y de movilidad. La Ley 2251 de 2022 ordena al RUNT publicar los vehículos involucrados en siniestros con muerte o lesión. Esa publicación es trazable: portal de datos abiertos, entidad responsable, licencia Creative Commons Attribution-ShareAlike 4.0 y fecha de actualización.

Cesar y Valledupar están en el archivo, con un volumen suficiente para describirlos y demasiado pequeño para estimar riesgo. Compararlos con el país muestra qué se puede decir a escala local y dónde el reporte nacional está distorsionado por filas repetidas.

PySpark organiza la carga, el esquema, las transformaciones y el Parquet con DataFrames. En este equipo Windows, el worker de Python que ejecuta `map` y `reduceByKey` cierra la conexión sin devolver resultados. La guía admite RDDs o DataFrames, y aquí el motor que sí se ejecuta es el DataFrame. El tamaño del archivo no exige un clúster. Kafka reproduce registros históricos ya procesados. El mensaje separa `received_at`, la hora de la simulación, de `fecha_accidente`, el mes real del siniestro. No es una transmisión de accidentes en vivo.

## Fuente

| Dato | Valor verificado |
|---|---|
| Portal | https://www.datos.gov.co/Transporte/VEHICULOS-INVOLUCRADOS-EN-UN-ACCIDENTE-DE-TRANSITO/6jmc-vaxk |
| Identificador | `6jmc-vaxk` |
| Título | VEHICULOS INVOLUCRADOS EN UN ACCIDENTE DE TRANSITO LEY 2251-2022 |
| Responsable | Ministerio de Transporte, RUNT 2.0 |
| Formato | CSV, UTF-8 |
| Tamaño | 42.642.682 bytes (40,7 MiB) |
| Filas | 450.335, más el encabezado |
| Columnas | 9 |
| Periodo | Enero de 2022 a septiembre de 2026, 57 meses consecutivos |
| Actualización del archivo | 4 de septiembre de 2026, 20:20 UTC |
| Cobertura declarada | Nacional |
| Frecuencia declarada | Semestral. La descripción del portal también habla de un consolidado anual |

Descarga: https://www.datos.gov.co/api/views/6jmc-vaxk/rows.csv?accessType=DOWNLOAD

Ubicación local: `data/raw/VEHICULOS_INVOLUCRADOS_EN_UN_ACCIDENTE_DE_TRANSITO_LEY_2251-2022.csv`

## Esquema

| Columna | Contenido observado |
|---|---|
| `MARCA_VEHICULO` | Texto. 381 marcas |
| `MODELO_VEHICULO` | Año de modelo, de 1937 a 2027 |
| `TIPO_VEHICULO` | 31 tipos. Motocicleta: 255.266 filas |
| `EDAD_VEHICULO` | Entero. 20 vacíos. La descripción oficial dice que es la edad desde la matrícula |
| `FECHA_ACCIDENTE` | Texto `MM/AAAA`, no una fecha diaria |
| `GRAVEDAD_ACCIDENTE` | `CON HERIDOS` (426.436) y `CON MUERTOS` (23.899) |
| `DEPARTAMENTO_ACCIDENTE` | 31 etiquetas, en mayúsculas y sin tilde |
| `MUNICIPIO_ACCIDENTE` | 663 etiquetas |
| `AUTORIDAD_DE_TRANSITO` | 237 autoridades |

No hay identificador del siniestro, placa, hora, coordenadas ni número de víctimas.

## Cobertura territorial

Aparecen 31 departamentos. No hay filas de Amazonas ni de Vaupés. Vichada tiene 2 y Guainía 67. Antioquia tiene 175.618 filas y Medellín 111.235.

Cesar tiene 1.985 filas, en 56 de los 57 meses. Valledupar, escrito `VALLEDUPAR` y solo dentro de `CESAR`, tiene 486 filas, en 52 meses: 353 con heridos y 133 con muertos. En el Cesar, los muertos son 644 de 1.985 filas.

`SAN MARTIN` aparece en Cesar (31) y en Meta (5). El filtro local usa departamento y municipio. Valledupar no tiene homónimo en este archivo.

En Cesar no aparecen los nombres Astrea, Chimichagua, Gamarra, González, La Gloria, Manaure Balcón del Cesar, Pueblo Bello ni Río de Oro. Hay dos etiquetas compuestas: `BOSCONIA - ALTO DE MINAS` (2) y `AGUSTÍN CODAZZI - CASACAZA` (1).

## Calidad que condiciona el análisis

Hay 290.936 combinaciones distintas y 159.399 filas que repiten una combinación ya vista. La más repetida aparece 3.778 veces: Kia, modelo 2011, automóvil, edad 15, julio de 2024, con heridos, Medellín, secretaría de tránsito de Medellín.

Julio de 2024 tiene 28.854 filas y 5.447 combinaciones. Medellín aporta 22.667 de esas filas y 737 combinaciones. Casi toda la repetición está en `CON HERIDOS`. Al contar combinaciones, la participación nacional de muertos pasa de 5,31 % a 8,01 %. Cesar casi no cambia: 1.985 filas y 1.961 combinaciones. Por eso la comparación de gravedad entre el país y Cesar se publica de las dos maneras.

65 edades superan 100 años y no coinciden con el modelo. Siete edades entre 81 y 87 corresponden a modelos de 1940 a 1946 y se conservan. Septiembre de 2026 tiene 41 filas.

Los conteos por año, en filas, son 115.201 (2022), 111.898 (2023), 121.050 (2024), 66.891 (2025) y 35.295 (2026). La caída de 2025 y 2026 no se interpreta como una reducción comprobada de la siniestralidad.

## Metodología de esta fase

1. Leer el CSV original con el esquema de nueve columnas de texto y comprobar el encabezado.
2. Convertir modelo, edad y mes sin imputar valores ausentes.
3. Marcar la multiplicidad de cada combinación y conservar todas las filas.
4. Calcular las distribuciones pedidas con DataFrames de PySpark.
5. Guardar la copia analítica en Parquet y las tablas y gráficos en `output/`.
6. Reproducir registros históricos en Kafka y agregarlos con Spark Structured Streaming por gravedad y por ventanas de recepción. La reproducción es histórica y no se presenta como accidentes en vivo.

## Limitaciones

El volumen no justifica hablar de Big Data a gran escala. El estudio describe este consolidado. No cubre choques solo con daños, no cuenta siniestros, no calcula tasas y no sostiene relaciones causales entre antigüedad, tipo y gravedad. Valledupar tiene bastante información para una descripción y poca para cruces finos o una serie mensual estable.
