"""
Facturación: extrae los datos de PostgreSQL y calcula los conceptos con calculos.py.

calcular_factura() es la función que pide la prueba: para un servicio y un mes
devuelve EA, EE1, EE2 y EC. calcular_concepto() calcula uno solo.
"""

from datetime import datetime

from app import calculos, db
from app.errores import DatosNoEncontrados


def rango_fechas(year, month):
    """Inicio (incluido) y fin (excluido) del mes."""
    if month == 12:
        return datetime(year, 12, 1), datetime(year + 1, 1, 1)
    return datetime(year, month, 1), datetime(year, month + 1, 1)


def datos_del_mes(conn, service_id, year, month):
    """Servicio con su tarifa y totales del mes. Falla si falta alguno."""
    servicio = db.obtener_servicio_y_tarifa(conn, service_id)
    if servicio is None:
        raise DatosNoEncontrados(f"No existe el servicio {service_id}")
    if servicio["cu"] is None:
        raise DatosNoEncontrados(
            f"No hay tarifa para el servicio {service_id} (mercado {servicio['id_market']}, "
            f"nivel de tensión {servicio['voltage_level']}, cdi {servicio['cdi']})"
        )

    inicio, fin = rango_fechas(year, month)
    totales = db.obtener_totales(conn, service_id, inicio, fin)
    if totales["horas"] == 0:
        raise DatosNoEncontrados(f"El servicio {service_id} no tiene mediciones en {year}-{month:02d}")

    return servicio, totales, inicio, fin


def calcular_ee2_con_bd(conn, service_id, totales, inicio, fin):
    """EE2 solo necesita el detalle hora a hora cuando la inyección supera el consumo."""
    if totales["inyeccion_total"] <= totales["consumo_total"]:
        horas = []
    else:
        horas = db.obtener_inyeccion_horaria(conn, service_id, inicio, fin)
    return calculos.calcular_ee2(totales["consumo_total"], horas)


def calcular_factura(service_id, year, month):
    """Calcula EA, EE1, EE2 y EC de un servicio en un mes, leyendo los datos de PostgreSQL."""
    with db.conectar() as conn:
        servicio, totales, inicio, fin = datos_del_mes(conn, service_id, year, month)
        consumo = totales["consumo_total"]
        inyeccion = totales["inyeccion_total"]
        conceptos = [
            calculos.calcular_ea(consumo, servicio["cu"]),
            calculos.calcular_ee1(consumo, inyeccion, servicio["cu"]),
            calcular_ee2_con_bd(conn, service_id, totales, inicio, fin),
            calculos.calcular_ec(inyeccion, servicio["c"]),
        ]

    return {
        "service_id": service_id,
        "year": year,
        "month": month,
        "conceptos": conceptos,
    }


def calcular_concepto(codigo, service_id, year, month):
    """Calcula un solo concepto (EA, EC, EE1 o EE2) de un servicio en un mes."""
    with db.conectar() as conn:
        servicio, totales, inicio, fin = datos_del_mes(conn, service_id, year, month)
        consumo = totales["consumo_total"]
        inyeccion = totales["inyeccion_total"]

        if codigo == "EA":
            return calculos.calcular_ea(consumo, servicio["cu"])
        if codigo == "EC":
            return calculos.calcular_ec(inyeccion, servicio["c"])
        if codigo == "EE1":
            return calculos.calcular_ee1(consumo, inyeccion, servicio["cu"])
        if codigo == "EE2":
            return calcular_ee2_con_bd(conn, service_id, totales, inicio, fin)

    raise ValueError(f"Concepto desconocido: {codigo}")
