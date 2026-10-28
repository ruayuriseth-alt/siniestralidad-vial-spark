"""Ejecuta la carga, la limpieza y el análisis exploratorio."""

import csv
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame

from src.batch.charts import (
    barras_horizontal,
    barras_vertical,
    comparacion_muertos,
    evolucion,
)
from src.batch.clean_data import (
    ORDEN_ANTIGUEDAD,
    calidad_crudo,
    calidad_limpio,
    es_cesar,
    limpiar,
)
from src.batch.exploratory_analysis import (
    categorias_frecuentes,
    combinacion_mas_repetida,
    comparacion_territorial,
    distribucion_antiguedad,
    distribucion_gravedad,
    distribucion_tipo,
    evolucion_temporal,
    filas_por_departamento,
    filas_por_municipio,
    municipios_compuestos,
)
from src.batch.load_data import leer_vehiculos
from src.common.paths import (
    NOMBRE_PARQUET,
    RUTA_GRAFICOS,
    RUTA_PROCESADOS,
    RUTA_RAW,
    RUTA_REPORTES,
)
from src.common.spark_session import crear_spark


def _registros(df: DataFrame) -> list[dict]:
    salida = []
    for fila in df.collect():
        registro = {}
        for clave, valor in fila.asDict().items():
            if hasattr(valor, "isoformat"):
                registro[clave] = valor.isoformat()
            else:
                registro[clave] = int(valor) if isinstance(valor, float) and valor.is_integer() else valor
        salida.append(registro)
    return salida


def _guardar_csv(ruta: Path, filas: list[dict], columnas: list[str]) -> None:
    ruta.parent.mkdir(parents=True, exist_ok=True)
    with ruta.open("w", encoding="utf-8-sig", newline="") as archivo:
        escritor = csv.DictWriter(archivo, fieldnames=columnas, extrasaction="ignore")
        escritor.writeheader()
        escritor.writerows(filas)


def _porcentaje(parte: int, total: int) -> str:
    if not total:
        return "0"
    return f"{100 * parte / total:.2f}".replace(".", ",")


def _entero(valor: int) -> str:
    return f"{valor:,}".replace(",", ".")


def _escribir_resumen(
    calidad_antes: dict,
    calidad_despues: dict,
    territorios: list[dict],
    repetida: dict,
    temporal: list[dict],
) -> None:
    meses_validos = [fila for fila in temporal if fila.get("fecha_mes")]
    primera = meses_validos[0]["fecha_mes"][:7] if meses_validos else "sin fechas"
    ultima = meses_validos[-1]["fecha_mes"][:7] if meses_validos else "sin fechas"
    lineas = [
        "# Resumen de la ejecución batch",
        "",
        f"Generado en UTC: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "Cada fila es un vehículo involucrado. La fuente no permite reconstruir siniestros únicos.",
        "",
        "## Calidad antes de convertir tipos",
        "",
        f"- Filas: {_entero(calidad_antes['filas'])}",
        f"- Columnas: {calidad_antes['columnas']}",
        f"- Combinaciones distintas de las nueve columnas: {_entero(calidad_antes['combinaciones'])}",
        (
            "- Filas adicionales que repiten una combinación ya observada: "
            f"{_entero(calidad_antes['filas_repetidas_adicionales'])}"
        ),
        "- Vacíos por columna:",
    ]
    for columna, vacios in calidad_antes["vacios"].items():
        lineas.append(f"  - {columna}: {_entero(vacios)}")
    lineas.extend(
        [
            "",
            "## Calidad después de la limpieza",
            "",
            "La limpieza no elimina filas. Separa tipos, marca edades inutilizables y calcula la multiplicidad.",
            "",
            f"- Filas: {_entero(calidad_despues['filas'])}",
            f"- Combinaciones distintas: {_entero(calidad_despues['combinaciones'])}",
            f"- Edad sin dato: {_entero(calidad_despues['edad_sin_dato'])}",
            f"- Edad no numérica: {_entero(calidad_despues['edad_no_numerica'])}",
            f"- Edad no utilizable (mayor de 100): {_entero(calidad_despues['edad_no_utilizable'])}",
            f"- Modelo no numérico: {_entero(calidad_despues['modelo_invalido'])}",
            f"- Fecha distinta de MM/AAAA: {_entero(calidad_despues['fecha_invalida'])}",
            f"- Municipios con complemento separado por ' - ': {_entero(calidad_despues['municipio_compuesto'])}",
            (
                f"- Julio de 2024: {_entero(calidad_despues['filas_julio_2024'])} filas y "
                f"{_entero(calidad_despues['combinaciones_julio_2024'])} combinaciones"
            ),
            "",
            "La combinación repetida más veces es:",
            "",
        ]
    )
    for clave, valor in repetida.items():
        lineas.append(f"- {clave}: {valor}")
    lineas.extend(["", "## Comparación territorial", ""])
    lineas.append(
        "| Ámbito | Filas | Combinaciones | Con heridos (filas) | Con muertos (filas) | Muertos sobre filas | Muertos sobre combinaciones | Motocicleta (filas) |"
    )
    lineas.append("|---|---:|---:|---:|---:|---:|---:|---:|")
    for fila in territorios:
        lineas.append(
            "| {ambito} | {filas} | {combinaciones} | {heridos} | {muertos} | {pct_filas} % | {pct_comb} % | {motos} |".format(
                ambito=fila["ambito"],
                filas=_entero(fila["filas"]),
                combinaciones=_entero(fila["combinaciones"]),
                heridos=_entero(fila["filas_con_heridos"]),
                muertos=_entero(fila["filas_con_muertos"]),
                pct_filas=_porcentaje(fila["filas_con_muertos"], fila["filas"]),
                pct_comb=_porcentaje(
                    fila["combinaciones_con_muertos"], fila["combinaciones"]
                ),
                motos=_entero(fila["filas_motocicleta"]),
            )
        )
    lineas.extend(
        [
            "",
            "Valledupar está contenido en Cesar. Sumar los tres ámbitos contaría dos veces los registros locales.",
            "",
            f"La serie mensual válida va de {primera} a {ultima} y tiene {len(meses_validos)} meses.",
            "",
            "El conteo por departamento se calcula con DataFrames. En este equipo, el worker de Python que ejecuta map y reduceByKey cierra la conexión sin devolver filas, así que esa vía de RDD no forma parte de la ejecución.",
            "",
            "## Criterios aplicados",
            "",
            "- El CSV original permanece en data/raw/.",
            "- Las fechas se interpretan como mes, con el patrón MM/AAAA. El orden cronológico usa la fecha derivada, no el texto.",
            "- La edad vacía queda como SIN DATO. La edad mayor de 100 queda fuera de las categorías de antigüedad utilizables y no se imputa.",
            "- Las edades de 81 a 87, cuando el modelo es antiguo, permanecen en la categoría 21 o mas.",
            "- Los filtros locales exigen la cadena CESAR y, para la ciudad, también VALLEDUPAR.",
            "- No se borran combinaciones repetidas. Las tablas publican filas y combinaciones.",
            "- El gráfico de volumen local compara Cesar y Valledupar. Colombia participa en el gráfico de gravedad, porque su número de filas usa otra escala.",
            "",
            "## Limitaciones",
            "",
            "- El archivo cubre vehículos en siniestros con muerte o lesión publicados por el RUNT. No representa todos los accidentes de Colombia.",
            "- Sin identificador de siniestro, las filas no se pueden convertir en accidentes.",
            "- La participación nacional de muertos cambia si se cuentan filas o combinaciones, por las repeticiones concentradas en Medellín en julio de 2024.",
            "- Cesar y Valledupar permiten una descripción. No permiten estimar riesgo ni una tendencia mensual estable.",
            "- Septiembre de 2026 debe leerse junto con la fecha de actualización del archivo, 4 de septiembre de 2026.",
            "",
        ]
    )
    RUTA_REPORTES.mkdir(parents=True, exist_ok=True)
    (RUTA_REPORTES / "resumen_ejecucion.md").write_text("\n".join(lineas), encoding="utf-8")


def _guardar_parquet(df: DataFrame) -> Path:
    destino = RUTA_PROCESADOS / NOMBRE_PARQUET
    if destino.exists():
        # Spark escribe un directorio. Se reemplaza completo en modo overwrite.
        pass
    RUTA_PROCESADOS.mkdir(parents=True, exist_ok=True)
    uri = "file:///" + destino.resolve().as_posix()
    df.write.mode("overwrite").parquet(uri)
    return destino


def main() -> None:
    spark = crear_spark()
    try:
        crudo = leer_vehiculos(spark, RUTA_RAW)
        crudo.cache()
        antes = calidad_crudo(crudo)
        print(f"Filas leídas: {antes['filas']}. Columnas: {antes['columnas']}")
        crudo.printSchema()
        crudo.show(5, truncate=False)

        limpio = limpiar(crudo).cache()
        despues = calidad_limpio(limpio)
        if despues["filas"] != antes["filas"]:
            raise RuntimeError(
                "La limpieza cambió el número de filas: "
                f"{antes['filas']} frente a {despues['filas']}"
            )
        if despues["combinaciones"] != antes["combinaciones"]:
            raise RuntimeError(
                "La multiplicidad no reproduce las combinaciones del archivo original: "
                f"{antes['combinaciones']} frente a {despues['combinaciones']}"
            )
        crudo.unpersist()

        departamentos = _registros(filas_por_departamento(limpio))
        municipios = _registros(filas_por_municipio(limpio))
        tipos = _registros(distribucion_tipo(limpio))
        gravedad = _registros(distribucion_gravedad(limpio))
        antiguedad = _registros(distribucion_antiguedad(limpio))
        temporal = _registros(evolucion_temporal(limpio))
        territorios = comparacion_territorial(limpio)
        marcas = _registros(categorias_frecuentes(limpio, "marca_vehiculo"))
        autoridades = _registros(categorias_frecuentes(limpio, "autoridad_de_transito"))
        compuestos = _registros(municipios_compuestos(limpio))
        cesar = _registros(filas_por_municipio(limpio.filter(es_cesar())))
        repetida = combinacion_mas_repetida(limpio)

        _guardar_csv(
            RUTA_REPORTES / "registros_por_departamento.csv",
            departamentos,
            ["departamento_accidente", "filas", "combinaciones"],
        )
        _guardar_csv(
            RUTA_REPORTES / "registros_por_municipio.csv",
            municipios,
            ["departamento_accidente", "municipio_accidente", "filas", "combinaciones"],
        )
        _guardar_csv(
            RUTA_REPORTES / "distribucion_tipo_vehiculo.csv",
            tipos,
            ["tipo_vehiculo", "filas", "combinaciones"],
        )
        _guardar_csv(
            RUTA_REPORTES / "distribucion_gravedad.csv",
            gravedad,
            ["gravedad_accidente", "filas", "combinaciones"],
        )
        _guardar_csv(
            RUTA_REPORTES / "distribucion_antiguedad.csv",
            antiguedad,
            ["categoria_antiguedad", "filas", "combinaciones"],
        )
        _guardar_csv(
            RUTA_REPORTES / "evolucion_temporal.csv",
            temporal,
            [
                "anio",
                "mes",
                "fecha_mes",
                "filas",
                "combinaciones",
                "filas_cesar",
                "combinaciones_cesar",
                "filas_valledupar",
                "combinaciones_valledupar",
            ],
        )
        columnas_territorio = [
            "ambito",
            "filas",
            "combinaciones",
            "filas_con_heridos",
            "filas_con_muertos",
            "combinaciones_con_heridos",
            "combinaciones_con_muertos",
            "filas_motocicleta",
        ]
        _guardar_csv(
            RUTA_REPORTES / "comparacion_territorial.csv",
            territorios,
            columnas_territorio,
        )
        _guardar_csv(
            RUTA_REPORTES / "marcas_frecuentes.csv",
            marcas,
            ["marca_vehiculo", "filas", "combinaciones"],
        )
        _guardar_csv(
            RUTA_REPORTES / "autoridades_frecuentes.csv",
            autoridades,
            ["autoridad_de_transito", "filas", "combinaciones"],
        )
        _guardar_csv(
            RUTA_REPORTES / "municipios_compuestos.csv",
            compuestos,
            [
                "departamento_accidente",
                "municipio_accidente",
                "municipio_base",
                "municipio_complemento",
                "municipio_base_normalizado",
                "filas",
            ],
        )
        _guardar_csv(
            RUTA_REPORTES / "cesar_por_municipio.csv",
            cesar,
            ["departamento_accidente", "municipio_accidente", "filas", "combinaciones"],
        )
        esquema = [
            f"{campo.name}: {campo.dataType.simpleString()}"
            for campo in limpio.schema.fields
        ]
        (RUTA_REPORTES / "esquema_limpio.txt").write_text(
            "\n".join(esquema) + "\n", encoding="utf-8"
        )
        _escribir_resumen(
            antes,
            despues,
            territorios,
            repetida,
            temporal,
        )

        barras_horizontal(
            [fila["departamento_accidente"] for fila in departamentos][::-1],
            [int(fila["filas"]) for fila in departamentos][::-1],
            RUTA_GRAFICOS / "departamentos_filas.png",
            "Vehículos registrados por departamento",
            "Filas publicadas",
        )
        principales = tipos[:12]
        resto = sum(int(fila["filas"]) for fila in tipos[12:])
        etiquetas_tipo = [fila["tipo_vehiculo"] for fila in principales]
        valores_tipo = [int(fila["filas"]) for fila in principales]
        if resto:
            etiquetas_tipo.append(f"Otros {len(tipos) - 12} tipos")
            valores_tipo.append(resto)
        barras_horizontal(
            etiquetas_tipo[::-1],
            valores_tipo[::-1],
            RUTA_GRAFICOS / "tipos_vehiculo.png",
            "Vehículos registrados por tipo",
            "Filas publicadas",
        )
        barras_vertical(
            [fila["gravedad_accidente"] for fila in gravedad],
            [int(fila["filas"]) for fila in gravedad],
            RUTA_GRAFICOS / "gravedad_nacional.png",
            "Gravedad de los vehículos registrados en Colombia",
            "Filas publicadas",
        )
        antiguedad_ordenada = sorted(
            antiguedad,
            key=lambda fila: ORDEN_ANTIGUEDAD.index(fila["categoria_antiguedad"]),
        )
        sin_dato = next(
            (
                int(fila["filas"])
                for fila in antiguedad_ordenada
                if fila["categoria_antiguedad"] == "SIN DATO"
            ),
            0,
        )
        no_utilizable = next(
            (
                int(fila["filas"])
                for fila in antiguedad_ordenada
                if fila["categoria_antiguedad"] == "EDAD NO UTILIZABLE"
            ),
            0,
        )
        barras_vertical(
            [fila["categoria_antiguedad"] for fila in antiguedad_ordenada],
            [int(fila["filas"]) for fila in antiguedad_ordenada],
            RUTA_GRAFICOS / "antiguedad_vehiculo.png",
            (
                "Antigüedad declarada del vehículo\n"
                f"Sin dato: {sin_dato}. Edad mayor de 100: {no_utilizable}. "
                "Esas cantidades son demasiado pequeñas para verse como barra."
            ),
            "Filas publicadas",
        )
        evolucion(temporal, RUTA_GRAFICOS / "evolucion_temporal.png")
        comparacion_muertos(territorios, RUTA_GRAFICOS / "gravedad_territorial.png")
        barras_horizontal(
            [fila["municipio_accidente"] for fila in cesar][::-1],
            [int(fila["filas"]) for fila in cesar][::-1],
            RUTA_GRAFICOS / "cesar_municipios.png",
            "Vehículos registrados en municipios del Cesar",
            "Filas publicadas",
        )
        locales = [fila for fila in territorios if fila["ambito"] != "Colombia"]
        barras_vertical(
            [fila["ambito"] for fila in locales],
            [int(fila["filas"]) for fila in locales],
            RUTA_GRAFICOS / "cesar_valledupar_filas.png",
            "Vehículos registrados en Cesar y Valledupar",
            "Filas publicadas",
        )
        principales_municipios = municipios[:15]
        barras_horizontal(
            [
                f"{fila['municipio_accidente']} ({fila['departamento_accidente']})"
                for fila in principales_municipios
            ][::-1],
            [int(fila["filas"]) for fila in principales_municipios][::-1],
            RUTA_GRAFICOS / "municipios_frecuentes.png",
            "15 municipios con más vehículos registrados",
            "Filas publicadas",
        )

        salida = limpio.drop("es_primera_combinacion")
        # La bandera de la primera fila de cada combinación depende de un orden interno.
        # Se conserva la multiplicidad, que sí es una propiedad de los datos.
        _guardar_parquet(salida)
        releido = spark.read.parquet(
            "file:///" + (RUTA_PROCESADOS / NOMBRE_PARQUET).resolve().as_posix()
        )
        if releido.count() != antes["filas"]:
            raise RuntimeError("El Parquet releído no conserva el número de filas.")
        print(f"Parquet verificado: {antes['filas']} filas.")
        print(f"Reportes: {RUTA_REPORTES}")
        print(f"Gráficos: {RUTA_GRAFICOS}")
        limpio.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
