"""Contrato JSON de la simulación, sin bróker ni Spark."""

from src.streaming.eventos import (
    AVISO,
    construir_evento,
    interpretar_mensaje,
    serializar_evento,
)


def _registro():
    return {
        "marca_vehiculo": "AKT",
        "modelo_texto": "2024",
        "modelo_vehiculo": 2024,
        "tipo_vehiculo": "MOTOCICLETA",
        "edad_texto": "3",
        "edad_vehiculo": 3,
        "categoria_antiguedad": "0-5",
        "fecha_accidente": "03/2024",
        "anio": 2024,
        "mes": 3,
        "gravedad_accidente": "CON HERIDOS",
        "departamento_accidente": "CESAR",
        "municipio_accidente": "VALLEDUPAR",
        "autoridad_de_transito": "AUTORIDAD CESAR",
        "multiplicidad": 2,
    }


def test_el_mensaje_separa_la_recepcion_de_la_fecha_historica():
    evento = construir_evento(_registro(), "evt-1", "2026-10-27T00:00:00Z")
    assert evento["simulacion"] is True
    assert evento["origen"] == "reproduccion_historica"
    assert evento["fecha_accidente"] == "03/2024"
    assert evento["received_at"] == "2026-10-27T00:00:00Z"
    assert evento["received_at"] != evento["fecha_accidente"]
    assert "SIMULACION ACADEMICA" in AVISO
    recuperado = interpretar_mensaje(serializar_evento(evento))
    assert recuperado["municipio_accidente"] == "VALLEDUPAR"
    assert recuperado["event_id"] == "evt-1"


def test_un_json_incompleto_o_malformado_no_es_utilizable():
    assert interpretar_mensaje("{") is None
    assert interpretar_mensaje("[]") is None
    assert interpretar_mensaje('{"event_id": "evt-1"}') is None
    assert (
        interpretar_mensaje(
            '{"event_id":"evt-1","received_at":"2026-10-27T00:00:00Z",'
            '"fecha_accidente":"03/2024","tipo_vehiculo":"","gravedad_accidente":"CON HERIDOS"}'
        )
        is None
    )
