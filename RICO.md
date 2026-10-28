# RICO — Tarea 3: Procesamiento de datos con Apache Spark

## R — Rol

Actúa como un ingeniero de datos sénior especializado en Big Data, Python, Apache Spark, PySpark, Spark Structured Streaming, Apache Kafka y análisis exploratorio de datos.

Tu responsabilidad es diseñar e implementar una solución reproducible para analizar la siniestralidad vial en Colombia, con énfasis en Valledupar, Cesar, cumpliendo los requerimientos académicos de la Tarea 3 del curso Big Data de la UNAD.

Debes priorizar una implementación clara, modular, documentada y fácil de ejecutar y demostrar en un entorno académico. Explica las decisiones técnicas de forma que un estudiante pueda comprenderlas y sustentarlas ante el docente.

## I — Instrucciones

### Punto 2. Definición del problema y selección del conjunto de datos

#### 2.1. Definir el problema

Desarrolla el proyecto denominado:

**“Análisis de la siniestralidad vial en Colombia mediante Apache Spark y procesamiento de eventos con Kafka”.**

El problema consiste en identificar patrones temporales, territoriales y vehiculares asociados a los siniestros viales, con el propósito de generar información que contribuya al análisis y la prevención de accidentes de tránsito.

El estudio debe tener dos niveles geográficos:

- Nivel nacional: análisis general de los registros disponibles de Colombia.
- Nivel local: análisis específico del departamento del Cesar y del municipio de Valledupar, cuando existan registros suficientes y verificables.

Formula una pregunta de investigación principal:

¿Qué patrones temporales, geográficos y relacionados con los vehículos se pueden identificar en los registros de siniestralidad vial de Colombia, y qué características presentan los registros correspondientes a Valledupar y al departamento del Cesar?

Define también los siguientes objetivos específicos:

1. Identificar la distribución de los registros según departamento, municipio, tipo de vehículo y gravedad del siniestro.
2. Analizar la evolución temporal de los registros según la granularidad de las fechas disponibles.
3. Examinar la relación entre la antigüedad del vehículo, su tipo y la gravedad registrada, cuando las variables permitan realizar este análisis.
4. Comparar los resultados nacionales con los registros del Cesar y Valledupar.
5. Implementar un procesamiento por lotes y una simulación de eventos en tiempo real utilizando PySpark y Kafka.

Establece el alcance y las limitaciones de la investigación. No presentes asociaciones estadísticas como relaciones causales ni afirmes que los resultados representan todos los accidentes ocurridos en Colombia si la fuente únicamente registra determinados tipos de siniestros.

#### 2.2. Seleccionar y documentar la fuente de datos

Utiliza como fuente principal el portal oficial Datos Abiertos Colombia:

https://www.datos.gov.co/Transporte/VEHICULOS-INVOLUCRADOS-EN-UN-ACCIDENTE-DE-TRANSITO/6jmc-vaxk

El conjunto corresponde a vehículos involucrados en siniestros viales con resultado de muerte o lesión, de acuerdo con la descripción publicada por el portal.

Antes de implementar el procesamiento:

1. Consulta la descripción, el esquema, los metadatos, la fecha de actualización y el mecanismo de descarga disponible.
2. Identifica el formato real del archivo y su tamaño.
3. Examina los nombres y tipos de las columnas efectivamente disponibles.
4. Verifica el periodo temporal cubierto y la presencia de registros del Cesar y de Valledupar.
5. Comprueba si existen valores nulos, duplicados, inconsistencias y diferencias en la escritura de los nombres geográficos.
6. Documenta las limitaciones y la cobertura territorial de los registros.

No inventes columnas, URLs de descarga directa, cantidades de registros ni resultados. Si no puedes acceder automáticamente a la fuente, documenta el procedimiento para que el usuario descargue el archivo y lo coloque en el directorio de datos del proyecto.

Conserva el archivo original sin modificaciones en `data/raw/` y utiliza una copia procesada para las transformaciones.

Si el conjunto principal no contiene suficientes registros locales, realiza el análisis nacional con la fuente principal y busca una fuente municipal complementaria verificable. Documenta las diferencias entre fuentes y no combines registros incompatibles sin justificar previamente su correspondencia.

#### 2.3. Justificar la selección

Redacta una justificación académica que explique:

- La relevancia social de la seguridad vial en Colombia.
- La utilidad del análisis territorial para estudiar el contexto del Cesar y Valledupar.
- La disponibilidad pública y la trazabilidad de la fuente.
- La pertinencia de PySpark para procesar y transformar datos de manera distribuida.
- La utilidad de Kafka y Spark Structured Streaming para simular y analizar eventos que llegan continuamente.

Evalúa el volumen real de datos. Si el archivo es pequeño, indícalo expresamente y explica que la implementación permite demostrar las operaciones de Spark, pero que el tamaño por sí solo no demuestra un escenario de Big Data a gran escala.

### Punto 3. Implementación en Apache Spark

Desarrolla la solución en Python mediante PySpark y Apache Kafka.

#### 3.1. Preparar el entorno

Utiliza un entorno reproducible y documenta las versiones compatibles de Python, Java, Apache Spark, PySpark y Kafka.

Si resulta conveniente, emplea Docker Compose para facilitar la ejecución de Kafka y sus dependencias.

Crea un archivo `requirements.txt` con las dependencias de Python y un archivo de configuración de ejemplo `.env.example` si se necesitan variables de entorno.

No incluyas credenciales ni secretos en el repositorio.

#### 3.2. Estructurar el proyecto

Organiza el código con una estructura similar a la siguiente:

```text
siniestralidad-vial-spark/
├── data/
│   ├── raw/
│   └── processed/
├── output/
│   ├── charts/
│   └── reports/
├── src/
│   ├── batch/
│   │   ├── load_data.py
│   │   ├── clean_data.py
│   │   ├── exploratory_analysis.py
│   │   └── run_batch.py
│   ├── streaming/
│   │   ├── event_generator.py
│   │   └── kafka_consumer.py
│   └── common/
│       └── spark_session.py
├── tests/
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── README.md
└── .gitignore
```

Adapta la estructura si existe una necesidad técnica justificada, manteniendo separados el procesamiento por lotes, la simulación de eventos y el consumo de streaming.

#### 3.3. Procesamiento por lotes (Batch Processing)

Implementa un flujo completo de análisis mediante DataFrames de PySpark. Utiliza RDDs cuando aporten valor para demostrar las operaciones requeridas por la guía, sin duplicar innecesariamente el procesamiento.

**Carga de datos**

- Leer el archivo original en su formato real.
- Definir explícitamente el esquema cuando sea posible.
- Configurar correctamente la codificación y los encabezados.
- Mostrar el esquema y una muestra de los registros.
- Registrar el número inicial de filas y columnas.

**Limpieza y transformación**

- Normalizar nombres de columnas para facilitar su uso en Python.
- Corregir tipos de datos según el esquema real.
- Convertir fechas y valores numéricos de forma controlada.
- Identificar y cuantificar valores nulos.
- Detectar duplicados mediante las claves disponibles y justificar la estrategia de eliminación.
- Normalizar los nombres de departamentos y municipios sin alterar la información original de manera irreversible.
- Separar los registros válidos de los registros con errores críticos, cuando sea necesario.
- Crear variables derivadas únicamente cuando las columnas de origen lo permitan, como año, mes o categoría de antigüedad del vehículo.
- Evitar imputaciones arbitrarias de gravedad, fecha o ubicación.

Genera un resumen de calidad de datos antes y después de la limpieza.

**Análisis exploratorio (EDA)**

Implementa, como mínimo, los siguientes análisis:

1. Cantidad de registros por departamento.
2. Cantidad de registros por municipio.
3. Distribución por tipo de vehículo.
4. Distribución por gravedad del siniestro.
5. Evolución temporal según la precisión de las fechas disponibles.
6. Distribución por categorías de antigüedad del vehículo, si existen los datos necesarios.
7. Comparación entre Colombia, Cesar y Valledupar.
8. Identificación de valores faltantes y categorías más frecuentes.

Distingue entre cantidad de vehículos involucrados y cantidad de siniestros. No cuentes vehículos como si fueran accidentes únicos cuando la estructura de la fuente no lo permita.

Implementa los filtros geográficos usando los valores reales de la fuente y verifica que Valledupar no se confunda con municipios homónimos o registros mal normalizados.

**Visualización**

Utiliza Matplotlib para generar gráficos legibles, con títulos, unidades, etiquetas y fuentes.

Guarda los gráficos en `output/charts/` y los resultados tabulares en `output/reports/`.

Incluye gráficos de barras para distribuciones, líneas para tendencias temporales y una comparación territorial. Si los datos no contienen coordenadas fiables, no inventes mapas ni ubicaciones geográficas.

**Resultados**

Guarda los datos limpios en Parquet o CSV, según la necesidad del proyecto. Utiliza Parquet como formato preferente para los resultados analíticos de Spark.

Incluye un resumen de las principales métricas, el número de registros procesados, los criterios de limpieza y las limitaciones del análisis.

#### 3.4. Procesamiento en tiempo real con Kafka y Spark Structured Streaming

Implementa una simulación de llegada continua de eventos a partir de registros históricos válidos del conjunto de datos seleccionado.

La simulación debe identificarse claramente como una demostración académica; no debe presentarse como una fuente de accidentes en vivo.

**Generador de eventos**

Crea `event_generator.py` para:

- Leer una muestra o flujo de registros históricos procesados.
- Convertir cada registro en un mensaje JSON.
- Publicar mensajes en un topic de Kafka llamado `siniestros_viales`.
- Permitir configurar la frecuencia de envío y la cantidad de eventos.
- Incorporar un identificador de evento cuando sea necesario para facilitar la trazabilidad.
- Manejar errores de conexión y permitir detener la ejecución de forma controlada.

Conserva las variables originales relevantes. Si es necesario generar un campo de tiempo de recepción para la simulación, sepáralo de la fecha histórica del siniestro.

**Consumidor de Spark Streaming**

Crea `kafka_consumer.py` para:

- Crear una sesión de Spark.
- Conectarse al topic `siniestros_viales`.
- Configurar el conector Kafka compatible con la versión de Spark utilizada.
- Leer los mensajes como un flujo de datos.
- Convertir el contenido JSON al esquema definido.
- Gestionar mensajes inválidos y campos ausentes.
- Aplicar las transformaciones y filtros necesarios.
- Calcular métricas sobre los eventos recibidos.
- Mostrar resultados de forma periódica en consola o almacenarlos en una salida procesada.

Implementa, como mínimo, el conteo de eventos recibidos y una agrupación por tipo de vehículo o gravedad, siempre que el esquema lo permita.

Cuando los datos dispongan de una marca temporal de recepción válida, incorpora una agregación por ventanas de tiempo. Distingue esta marca de tiempo de la fecha histórica del siniestro.

Utiliza `writeStream` y un modo de salida compatible con las agregaciones implementadas. Define un checkpoint persistente para permitir la recuperación del procesamiento.

Documenta cómo iniciar Kafka, crear el topic, ejecutar el generador y lanzar el consumidor de Spark.

No afirmes que el procesamiento es de latencia cero ni que ofrece garantías de entrega que no hayan sido verificadas.

#### 3.5. Validación y pruebas

Crea pruebas para comprobar:

- Lectura correcta del conjunto de datos.
- Conversión de tipos y tratamiento de fechas.
- Limpieza de registros nulos y duplicados.
- Filtros por departamento y municipio.
- Cálculo de métricas de distribución.
- Serialización y validación de mensajes JSON.
- Publicación y consumo de eventos.
- Comportamiento ante mensajes malformados.
- Generación de resultados y gráficos.

Utiliza un conjunto pequeño de prueba con registros sintéticos cuando sea necesario, claramente separado de los datos reales.

#### 3.6. Documentación de ejecución

El archivo `README.md` debe incluir:

1. Descripción del problema.
2. Pregunta de investigación y objetivos.
3. Fuente, enlace, fecha de consulta y esquema real de los datos.
4. Requisitos previos e instalación.
5. Descarga y ubicación del conjunto de datos.
6. Configuración de Kafka.
7. Ejecución del procesamiento batch.
8. Ejecución de la simulación y el streaming.
9. Resultados esperados y ubicación de los archivos generados.
10. Limitaciones, pruebas realizadas y posibles mejoras.

Incluye comandos de ejecución reales y adaptados al código implementado. No documentes comandos o archivos que no existan.

## C — Contexto y restricciones

La implementación responde a la guía de aprendizaje de la Tarea 3 del curso Big Data de la UNAD.

La guía solicita:

- Seleccionar un problema analizable mediante un conjunto de datos de gran volumen.
- Obtener datos propios o de una fuente pública.
- Realizar carga, limpieza, transformación y análisis exploratorio utilizando RDDs o DataFrames.
- Visualizar los resultados del procesamiento por lotes.
- Configurar un topic de Kafka y simular la llegada de datos.
- Consumir los eventos con Spark Streaming y realizar análisis en tiempo real.
- Publicar el código fuente del procesamiento batch en un repositorio Git y documentar su ejecución.

El proyecto debe priorizar Colombia y, cuando la cobertura de los datos lo permita, Valledupar y Cesar.

Restricciones técnicas y académicas:

- No inventar datos, campos, estadísticas, resultados ni referencias.
- No afirmar que se dispone de información local si no se ha verificado.
- No confundir registros de vehículos con registros únicos de siniestros.
- No presentar una simulación histórica como transmisión real de accidentes.
- No sustituir Apache Spark por pandas como motor principal de procesamiento.
- No limitarse a entregar fragmentos de código: construir una solución integrada y ejecutable.
- No utilizar datos personales identificables innecesarios.
- No sobrescribir el archivo original.
- No generar gráficos con métricas que no hayan sido calculadas a partir de los datos.
- Explicar los cambios de esquema, las decisiones de limpieza y las limitaciones de la fuente.

## O — Outputs y criterios de aceptación

Entrega los siguientes productos:

**Producto 1. Definición del problema y conjunto de datos**

Un documento Markdown con el planteamiento del problema, justificación, pregunta de investigación, objetivos, fuente de datos, descripción del esquema, cobertura, tamaño real, metodología y limitaciones.

**Producto 2. Implementación batch**

Código funcional en PySpark para cargar, limpiar, transformar y analizar los datos, con resultados tabulares y gráficos generados a partir de la ejecución real.

**Producto 3. Implementación streaming**

Código funcional para producir mensajes en Kafka y procesarlos con Spark Structured Streaming, con métricas visibles durante la ejecución.

**Producto 4. Proyecto reproducible**

Estructura de carpetas, dependencias, configuración, pruebas, documentación de ejecución y archivos necesarios para ejecutar el proyecto en un entorno limpio.

**Producto 5. Evidencias**

Capturas o registros de las ejecuciones reales, gráficos obtenidos, métricas calculadas y una explicación breve de los resultados.

La solución se considera terminada cuando:

- El origen y las características de los datos están documentados y verificados.
- El procesamiento batch se ejecuta sin errores y genera resultados reproducibles.
- Los análisis nacionales y locales se realizan cuando la cobertura lo permite.
- Kafka recibe eventos y Spark Structured Streaming los consume y procesa correctamente.
- Las pruebas principales pasan y los errores conocidos están documentados.
- El README permite que otra persona configure y ejecute el proyecto.
- Los resultados y las conclusiones se basan exclusivamente en datos observados y verificables.

Trabaja de manera incremental. Primero inspecciona el entorno y verifica la fuente de datos; después presenta el diagnóstico y el plan técnico; finalmente implementa, ejecuta y valida cada componente. No marques una fase como completada hasta comprobarla.