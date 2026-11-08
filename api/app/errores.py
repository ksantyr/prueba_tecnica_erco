"""Errores de negocio. main.py los convierte en respuestas HTTP."""


class DatosNoEncontrados(Exception):
    """No existe el servicio, su tarifa o mediciones en el periodo pedido (HTTP 404)."""


class DatosIncompletos(Exception):
    """Faltan datos para poder calcular, por ejemplo el valor de XM de una hora (HTTP 422)."""
