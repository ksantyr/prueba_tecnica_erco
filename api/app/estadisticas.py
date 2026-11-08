"""Estadísticas de consumo e inyección por servicio y carga del sistema por hora."""

from app import db
from app.errores import DatosNoEncontrados


def estadisticas_servicio(service_id):
    """Total, promedio, máximo y mínimo horario de consumo e inyección del servicio."""
    with db.conectar() as conn:
        if not db.existe_servicio(conn, service_id):
            raise DatosNoEncontrados(f"No existe el servicio {service_id}")
        fila = db.obtener_estadisticas(conn, service_id)

    if fila["horas"] == 0:
        raise DatosNoEncontrados(f"El servicio {service_id} no tiene mediciones")

    return {
        "service_id": service_id,
        "horas_con_medicion": fila["horas"],
        "consumo": {
            "total_kwh": fila["consumo_total"],
            "promedio_horario_kwh": fila["consumo_promedio"],
            "maximo_horario_kwh": fila["consumo_maximo"],
            "minimo_horario_kwh": fila["consumo_minimo"],
        },
        "inyeccion": {
            "total_kwh": fila["inyeccion_total"],
            "promedio_horario_kwh": fila["inyeccion_promedio"],
            "maximo_horario_kwh": fila["inyeccion_maximo"],
            "minimo_horario_kwh": fila["inyeccion_minimo"],
        },
    }


def carga_sistema():
    """Consumo de todos los servicios sumado en cada hora."""
    with db.conectar() as conn:
        horas = db.obtener_carga_por_hora(conn)

    if not horas:
        raise DatosNoEncontrados("No hay mediciones de consumo")
    return horas
