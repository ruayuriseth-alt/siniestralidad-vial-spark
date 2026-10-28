"""Gráficos de las métricas ya calculadas. No agregan cifras nuevas."""

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

FUENTE = (
    "Fuente: RUNT, Datos Abiertos Colombia, conjunto 6jmc-vaxk. "
    "Unidad: vehículos involucrados, no siniestros únicos."
)


def _miles(valor: float, _posicion: int) -> str:
    return f"{int(valor):,}".replace(",", ".")


def _cerrar(figura: plt.Figure, ruta: Path) -> None:
    ruta.parent.mkdir(parents=True, exist_ok=True)
    figura.savefig(ruta, dpi=140, bbox_inches="tight", pad_inches=0.35, facecolor="white")
    plt.close(figura)


def _nota(eje: plt.Axes, descenso: float = -0.18) -> None:
    eje.annotate(
        FUENTE,
        xy=(0, descenso),
        xycoords="axes fraction",
        fontsize=8,
        color="#444444",
        annotation_clip=False,
    )


def barras_horizontal(
    etiquetas: list[str],
    valores: list[int],
    ruta: Path,
    titulo: str,
    etiqueta_x: str,
) -> None:
    figura, eje = plt.subplots(figsize=(11, max(4.5, 0.38 * len(etiquetas) + 1.6)))
    eje.barh(etiquetas, valores, color="#1f4e79")
    eje.set_title(titulo)
    eje.set_xlabel(etiqueta_x)
    eje.xaxis.set_major_formatter(plt.FuncFormatter(_miles))
    eje.grid(axis="x", linestyle=":", alpha=0.6)
    _nota(eje)
    _cerrar(figura, ruta)


def barras_vertical(
    etiquetas: list[str],
    valores: list[int],
    ruta: Path,
    titulo: str,
    etiqueta_y: str,
) -> None:
    figura, eje = plt.subplots(figsize=(11, 6))
    eje.bar(etiquetas, valores, color="#1f4e79")
    eje.set_title(titulo)
    eje.set_ylabel(etiqueta_y)
    eje.yaxis.set_major_formatter(plt.FuncFormatter(_miles))
    eje.tick_params(axis="x", rotation=35)
    eje.grid(axis="y", linestyle=":", alpha=0.6)
    _nota(eje, -0.32)
    _cerrar(figura, ruta)


def evolucion(filas_tiempo: list[dict], ruta: Path) -> None:
    etiquetas = [
        f"{int(fila['anio']):04d}-{int(fila['mes']):02d}"
        for fila in filas_tiempo
        if fila.get("anio") is not None
    ]
    serie_filas = [int(fila["filas"]) for fila in filas_tiempo if fila.get("anio") is not None]
    serie_combinaciones = [
        int(fila["combinaciones"])
        for fila in filas_tiempo
        if fila.get("anio") is not None
    ]
    figura, eje = plt.subplots(figsize=(12, 6))
    eje.plot(etiquetas, serie_filas, color="#1f4e79", label="Filas publicadas")
    eje.plot(
        etiquetas,
        serie_combinaciones,
        color="#c45911",
        linestyle="--",
        label="Combinaciones distintas",
    )
    eje.set_title("Vehículos registrados por mes, Colombia")
    eje.set_ylabel("Cantidad")
    eje.set_xlabel("Mes del siniestro")
    eje.yaxis.set_major_formatter(plt.FuncFormatter(_miles))
    eje.grid(linestyle=":", alpha=0.6)
    eje.legend()
    paso = 3 if len(etiquetas) > 24 else 1
    eje.set_xticks(range(0, len(etiquetas), paso))
    eje.set_xticklabels(etiquetas[::paso], rotation=45, ha="right")
    _nota(eje, -0.46)
    _cerrar(figura, ruta)


def comparacion_muertos(territorios: list[dict], ruta: Path) -> None:
    """Compara la participación de CON MUERTOS en filas y en combinaciones."""
    ambitos = [fila["ambito"] for fila in territorios]
    porcentaje_filas = [
        (100 * fila["filas_con_muertos"] / fila["filas"]) if fila["filas"] else 0
        for fila in territorios
    ]
    porcentaje_combinaciones = [
        (
            100
            * fila["combinaciones_con_muertos"]
            / fila["combinaciones"]
        )
        if fila["combinaciones"]
        else 0
        for fila in territorios
    ]
    posiciones = range(len(ambitos))
    ancho = 0.38
    figura, eje = plt.subplots(figsize=(10, 6))
    eje.bar(
        [p - ancho / 2 for p in posiciones],
        porcentaje_filas,
        width=ancho,
        color="#1f4e79",
        label="Sobre filas publicadas",
    )
    eje.bar(
        [p + ancho / 2 for p in posiciones],
        porcentaje_combinaciones,
        width=ancho,
        color="#c45911",
        label="Sobre combinaciones distintas",
    )
    eje.set_xticks(list(posiciones))
    eje.set_xticklabels(ambitos)
    eje.set_ylabel("Participación de registros CON MUERTOS (%)")
    eje.set_title("Gravedad por territorio")
    eje.grid(axis="y", linestyle=":", alpha=0.6)
    eje.legend()
    _nota(eje, -0.2)
    _cerrar(figura, ruta)
