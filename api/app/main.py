"""
API REST de facturación de energía.

Documentación interactiva: http://localhost:8000/docs (Swagger) y http://localhost:8000/redoc
"""

import psycopg
from fastapi import FastAPI, Path
from fastapi.responses import JSONResponse

from app import estadisticas, facturacion
from app.errores import DatosIncompletos, DatosNoEncontrados
from app.modelos import (
    CargaHora,
    Concepto,
    ConceptoEE2,
    Error,
    EstadisticasServicio,
    Factura,
    SolicitudFactura,
)

app = FastAPI(
    title="ERCO - API de facturación de energía",
    version="1.0.0",
    description=(
        "Calcula los conceptos de la factura de energía de un servicio en un mes (EA, EE1, EE2 y EC) "
        "y entrega estadísticas de consumo e inyección a partir de la base PostgreSQL erco."
    ),
)

ERRORES = {
    404: {"model": Error, "description": "No existe el servicio, su tarifa o sus mediciones"},
    422: {"description": "Parámetros inválidos o datos incompletos para calcular"},
    503: {"model": Error, "description": "No se pudo conectar a la base de datos"},
}


@app.exception_handler(DatosNoEncontrados)
def manejar_no_encontrados(request, error):
    return JSONResponse(status_code=404, content={"detail": str(error)})


@app.exception_handler(DatosIncompletos)
def manejar_datos_incompletos(request, error):
    return JSONResponse(status_code=422, content={"detail": str(error)})


@app.exception_handler(psycopg.OperationalError)
def manejar_error_conexion(request, error):
    return JSONResponse(status_code=503, content={"detail": "No se pudo conectar a la base de datos"})


# ---------------------------------------------------------------------------
# Facturación
# ---------------------------------------------------------------------------

@app.post("/calculate-invoice", response_model=Factura, responses=ERRORES, tags=["Facturación"], summary="Calcula la factura de un servicio en un mes")
def calculate_invoice(solicitud: SolicitudFactura):
    """Devuelve los cuatro conceptos de la factura: EA, EE1, EE2 y EC."""
    return facturacion.calcular_factura(solicitud.service_id, solicitud.year, solicitud.month)


@app.post("/calculate-ea", response_model=Concepto, responses=ERRORES, tags=["Conceptos"], summary="Energía Activa (EA)")
def calculate_ea(solicitud: SolicitudFactura):
    """Consumo total del mes x CU."""
    return facturacion.calcular_concepto("EA", solicitud.service_id, solicitud.year, solicitud.month)


@app.post("/calculate-ee1", response_model=Concepto, responses=ERRORES, tags=["Conceptos"], summary="Excedentes de Energía tipo 1 (EE1)")
def calculate_ee1(solicitud: SolicitudFactura):
    """La inyección hasta el consumo total del mes, con tarifa -CU."""
    return facturacion.calcular_concepto("EE1", solicitud.service_id, solicitud.year, solicitud.month)


@app.post("/calculate-ee2", response_model=ConceptoEE2, responses=ERRORES, tags=["Conceptos"], summary="Excedentes de Energía tipo 2 (EE2)")
def calculate_ee2(solicitud: SolicitudFactura):
    """La inyección que supera el consumo total del mes, valorada hora a hora con xm_data_hourly_per_agent."""
    return facturacion.calcular_concepto("EE2", solicitud.service_id, solicitud.year, solicitud.month)


@app.post("/calculate-ec", response_model=Concepto, responses=ERRORES, tags=["Conceptos"], summary="Comercialización de Excedentes de Energía (EC)")
def calculate_ec(solicitud: SolicitudFactura):
    """Inyección total del mes x C."""
    return facturacion.calcular_concepto("EC", solicitud.service_id, solicitud.year, solicitud.month)


# ---------------------------------------------------------------------------
# Estadísticas
# ---------------------------------------------------------------------------

@app.get("/client-statistics/{service_id}", response_model=EstadisticasServicio, responses=ERRORES, tags=["Estadísticas"], summary="Estadísticas de consumo e inyección de un servicio")
def client_statistics(service_id: int = Path(gt=0, description="Identificador del servicio (id_service)")):
    """Total, promedio, máximo y mínimo horario de consumo e inyección del servicio."""
    return estadisticas.estadisticas_servicio(service_id)


@app.get("/system-load", response_model=list[CargaHora], responses=ERRORES, tags=["Estadísticas"], summary="Carga del sistema por hora")
def system_load():
    """Consumo de todos los servicios sumado en cada hora."""
    return estadisticas.carga_sistema()
