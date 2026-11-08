"""
Cálculo de los conceptos de una factura de energía: EA, EC, EE1 y EE2.

Cada concepto es una cantidad de energía (kWh) por una tarifa ($/kWh).
Estas funciones no consultan la base de datos: reciben los datos ya extraídos
(ver facturacion.py), así se pueden probar de forma aislada.
Los valores llegan como Decimal desde PostgreSQL para no perder precisión.
"""

from decimal import Decimal
from app.errores import DatosIncompletos

CERO = Decimal("0")


def armar_concepto(codigo, descripcion, cantidad, tarifa, monto):
    """Devuelve el concepto como diccionario, con cantidad, tarifa y monto redondeados."""
    if tarifa is not None:
        tarifa = round(tarifa, 6)
    return {
        "concepto": codigo,
        "descripcion": descripcion,
        "cantidad_kwh": round(cantidad, 6),
        "tarifa": tarifa,
        "monto": round(monto, 2),
    }


def calcular_ea(consumo_total, cu):
    """Energía Activa: consumo total del mes x CU."""
    monto = consumo_total * cu
    return armar_concepto("EA", "Energía Activa", consumo_total, cu, monto)


def calcular_ec(inyeccion_total, c):
    """Comercialización de Excedentes de Energía: inyección total del mes x C."""
    monto = inyeccion_total * c
    return armar_concepto("EC", "Comercialización de Excedentes de Energía", inyeccion_total, c, monto)


def calcular_ee1(consumo_total, inyeccion_total, cu):
    """
    Excedentes de Energía tipo 1, con tarifa -CU.

    Si la inyección es menor o igual al consumo, la cantidad es toda la inyección;
    si es mayor, la cantidad es el consumo (el resto es EE2).
    """
    if inyeccion_total <= consumo_total:
        cantidad = inyeccion_total
    else:
        cantidad = consumo_total
    tarifa = -cu
    return armar_concepto("EE1", "Excedentes de Energía tipo 1", cantidad, tarifa, cantidad * tarifa)


def calcular_ee2(consumo_total, horas):
    """
    Excedentes de Energía tipo 2: la inyección que supera el consumo total del mes.

    horas: lista en orden cronológico; cada elemento tiene record_timestamp,
    inyeccion y tarifa_xm (valor de xm_data_hourly_per_agent en esa hora).

    Se acumula la inyección hora a hora. Mientras el acumulado no pase el consumo
    total, esa energía es EE1. Desde la hora en que lo pasa, el exceso de cada hora
    se multiplica por el valor de XM de esa hora y se suman los resultados.
    Si la inyección total no supera el consumo, EE2 es cero.
    """
    acumulado = CERO
    cantidad = CERO
    monto = CERO
    detalle = []

    for hora in horas:
        anterior = acumulado
        acumulado = acumulado + hora["inyeccion"]
        if acumulado <= consumo_total:
            continue

        # Exceso de esta hora: lo que pasa del consumo total. Si el consumo ya se
        # había superado en horas anteriores, es toda la inyección de la hora.
        exceso = acumulado - max(anterior, consumo_total)
        if exceso == 0:
            continue
        if hora["tarifa_xm"] is None:
            raise DatosIncompletos(f"Falta el valor de XM para la hora {hora['record_timestamp']}")

        monto_hora = exceso * hora["tarifa_xm"]
        cantidad = cantidad + exceso
        monto = monto + monto_hora
        detalle.append({
            "record_timestamp": hora["record_timestamp"],
            "exceso_kwh": round(exceso, 6),
            "tarifa_xm": hora["tarifa_xm"],
            "monto": round(monto_hora, 6),  # sin redondear a centavos: la suma de las horas da el monto de EE2
        })

    # EE2 no tiene una tarifa única: cambia hora a hora (ver detalle_horario)
    concepto = armar_concepto("EE2", "Excedentes de Energía tipo 2", cantidad, None, monto)
    concepto["detalle_horario"] = detalle
    return concepto
