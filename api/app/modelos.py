"""Formato de las peticiones y respuestas de la API (también genera la documentación en /docs)."""

from datetime import datetime
from pydantic import BaseModel, Field


class SolicitudFactura(BaseModel):
    service_id: int = Field(gt=0, description="Identificador del servicio (id_service)", examples=[3222])
    year: int = Field(ge=2000, le=2100, description="Año de la factura", examples=[2023])
    month: int = Field(ge=1, le=12, description="Mes de la factura (1 a 12)", examples=[9])


class Concepto(BaseModel):
    concepto: str = Field(description="Código del concepto: EA, EE1, EE2 o EC")
    descripcion: str
    cantidad_kwh: float = Field(description="Cantidad de energía en kWh")
    tarifa: float | None = Field(
        description="Tarifa en $/kWh. En EE2 es null: la tarifa cambia hora a hora (ver detalle_horario en /calculate-ee2)"
    )
    monto: float = Field(description="Cantidad x tarifa, en $. En EE2, la suma hora a hora de exceso x tarifa XM")


class HoraEE2(BaseModel):
    record_timestamp: datetime
    exceso_kwh: float = Field(description="Energía de la hora que supera el consumo total del mes")
    tarifa_xm: float = Field(description="Valor de xm_data_hourly_per_agent en esa hora")
    monto: float = Field(description="exceso_kwh x tarifa_xm")


class ConceptoEE2(Concepto):
    detalle_horario: list[HoraEE2] = Field(description="Horas con excedente tipo 2 y su valoración")


class Factura(BaseModel):
    service_id: int
    year: int
    month: int
    conceptos: list[Concepto] = Field(description="EA, EE1, EE2 y EC")


class Medida(BaseModel):
    total_kwh: float
    promedio_horario_kwh: float | None
    maximo_horario_kwh: float | None
    minimo_horario_kwh: float | None


class EstadisticasServicio(BaseModel):
    service_id: int
    horas_con_medicion: int
    consumo: Medida
    inyeccion: Medida


class CargaHora(BaseModel):
    record_timestamp: datetime
    carga_kwh: float = Field(description="Consumo de todos los servicios en la hora")


class Error(BaseModel):
    detail: str
