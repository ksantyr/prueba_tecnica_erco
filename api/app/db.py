"""
Acceso a PostgreSQL: conexión y consultas.

La API usa el usuario erco_lectura, que solo puede leer las vistas vw_*.
Las fechas se filtran con rangos (>= inicio y < fin) para que PostgreSQL use
los índices de record_timestamp y (id_service, record_timestamp).
"""

import os

import psycopg
from psycopg.rows import dict_row

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://erco_lectura:erco_lectura@localhost:5432/erco"
)


def conectar():
    """Abre una conexión que devuelve cada fila como diccionario."""
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def existe_servicio(conn, service_id):
    sql = "SELECT 1 FROM vw_services WHERE id_service = %(service_id)s"
    return conn.execute(sql, {"service_id": service_id}).fetchone() is not None


def obtener_servicio_y_tarifa(conn, service_id):
    """Servicio con su tarifa. En niveles de tensión 2 y 3 el cdi no se usa para el cruce."""
    sql = """
        SELECT s.id_service, s.id_market, s.voltage_level, s.cdi, t.cu, t.c
        FROM vw_services s
        LEFT JOIN vw_tariffs t
               ON t.id_market = s.id_market
              AND t.voltage_level = s.voltage_level
              AND (s.voltage_level IN (2, 3) OR t.cdi = s.cdi)
        WHERE s.id_service = %(service_id)s
    """
    return conn.execute(sql, {"service_id": service_id}).fetchone()


def obtener_totales(conn, service_id, inicio, fin):
    """Horas con medición y suma de consumo e inyección del servicio en el rango."""
    sql = """
        SELECT count(*) AS horas,
               coalesce(sum(c.value), 0) AS consumo_total,
               coalesce(sum(i.value), 0) AS inyeccion_total
        FROM vw_records r
        LEFT JOIN vw_consumption c ON c.id_record = r.id_record
        LEFT JOIN vw_injection i ON i.id_record = r.id_record
        WHERE r.id_service = %(service_id)s
          AND r.record_timestamp >= %(inicio)s
          AND r.record_timestamp < %(fin)s
    """
    parametros = {"service_id": service_id, "inicio": inicio, "fin": fin}
    return conn.execute(sql, parametros).fetchone()


def obtener_inyeccion_horaria(conn, service_id, inicio, fin):
    """Inyección hora a hora del servicio con el valor de XM de esa hora, en orden cronológico."""
    sql = """
        SELECT r.record_timestamp,
               coalesce(i.value, 0) AS inyeccion,
               x.value AS tarifa_xm
        FROM vw_records r
        LEFT JOIN vw_injection i ON i.id_record = r.id_record
        LEFT JOIN vw_xm_data_hourly_per_agent x ON x.record_timestamp = r.record_timestamp
        WHERE r.id_service = %(service_id)s
          AND r.record_timestamp >= %(inicio)s
          AND r.record_timestamp < %(fin)s
        ORDER BY r.record_timestamp
    """
    parametros = {"service_id": service_id, "inicio": inicio, "fin": fin}
    return conn.execute(sql, parametros).fetchall()


def obtener_estadisticas(conn, service_id):
    """Total, promedio, máximo y mínimo horario de consumo e inyección del servicio."""
    sql = """
        SELECT count(*) AS horas,
               coalesce(sum(c.value), 0) AS consumo_total,
               round(avg(c.value), 6) AS consumo_promedio,
               max(c.value) AS consumo_maximo,
               min(c.value) AS consumo_minimo,
               coalesce(sum(i.value), 0) AS inyeccion_total,
               round(avg(i.value), 6) AS inyeccion_promedio,
               max(i.value) AS inyeccion_maximo,
               min(i.value) AS inyeccion_minimo
        FROM vw_records r
        LEFT JOIN vw_consumption c ON c.id_record = r.id_record
        LEFT JOIN vw_injection i ON i.id_record = r.id_record
        WHERE r.id_service = %(service_id)s
    """
    return conn.execute(sql, {"service_id": service_id}).fetchone()


def obtener_carga_por_hora(conn):
    """Consumo total de todos los servicios en cada hora (carga del sistema)."""
    sql = """
        SELECT r.record_timestamp,
               sum(c.value) AS carga_kwh
        FROM vw_records r
        JOIN vw_consumption c ON c.id_record = r.id_record
        GROUP BY r.record_timestamp
        ORDER BY r.record_timestamp
    """
    return conn.execute(sql).fetchall()
