"""Contrato JSON de la simulación. No representa accidentes en vivo."""

import json
from datetime import datetime, timezone

CAMPOS_REGISTRO = (
    "marca_vehiculo",
    "modelo_texto",
    "modelo_vehiculo",
    "tipo_vehiculo",
    "edad_texto",
    "edad_vehiculo",
    "categoria_antiguedad",
    "fecha_accidente",
    "anio",
    "mes",
    "gravedad_accidente",
    "departamento_accidente",
    "municipio_accidente",
    "autoridad_de_transito",
    "multiplicidad",
)

CAMPOS_OBLIGATORIOS = (
    "event_id",
    "received_at",
    "fecha_accidente",
    "tipo_vehiculo",
    "gravedad_accidente",
)

TOPIC = "siniestros_viales"
AVISO = (
    "SIMULACION ACADEMICA. No es un flujo de accidentes en vivo. "
    "Los mensajes reproducen registros historicos del RUNT. "
    "received_at es la hora de envio; fecha_accidente es el mes historico del siniestro."
)


def marca_de_tiempo() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _valor_json(valor):
    if valor is None:
        return None
    if hasattr(valor, "item"):
        return valor.item()
    if isinstance(valor, datetime):
        return valor.date().isoformat()
    return valor


def construir_evento(registro: dict, event_id: str, received_at: str) -> dict:
    """Arma un mensaje con la hora de recepción separada de la fecha histórica."""
    evento = {
        "event_id": event_id,
        "received_at": received_at,
        "simulacion": True,
        "origen": "reproduccion_historica",
    }
    for campo in CAMPOS_REGISTRO:
        evento[campo] = _valor_json(registro.get(campo))
    return evento


def serializar_evento(evento: dict) -> str:
    return json.dumps(evento, ensure_ascii=False)


def interpretar_mensaje(contenido: str) -> dict | None:
    """Devuelve el evento si el JSON trae los campos usados por el consumidor."""
    try:
        dato = json.loads(contenido)
    except json.JSONDecodeError:
        return None
    if not isinstance(dato, dict):
        return None
    if any(dato.get(campo) in (None, "") for campo in CAMPOS_OBLIGATORIOS):
        return None
    return dato
