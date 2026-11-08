"""
### erco_carga_tariffs
Carga diaria (08:00 hora Colombia) del archivo `data/tariffs 4.csv` a la tabla `tariffs`.

`extraer` → `transformar` → `cargar`

- **extraer**: lee el archivo, valida el encabezado y guarda `<dag_id>_extraido.parquet`.
- **transformar**: tipa y valida los datos, guarda `<dag_id>_transformado.parquet` y borra `<dag_id>_extraido.parquet`.
  - Encabezados G, T, D, R, C, P, CU pasan a minúscula.
  - En niveles de tensión 2 y 3 el cdi no aplica: se guarda como NULL.
  - Se advierte si CU no es igual a G + T + D + R + C + P.
- **cargar**: UPSERT en la tabla guardando `source_file` y los datos de control
  (`created_by` / `updated_by` = `USUARIO_ETL`, `updated_at`), y borra `<dag_id>_transformado.parquet`.

Los archivos temporales se guardan en `/opt/airflow/tmp/erco/` con el nombre del DAG
(`erco_carga_tariffs_extraido.parquet`, `erco_carga_tariffs_transformado.parquet`); no se usa XCom.
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


# =========================================================================== #
# CONFIGURACIÓN                                                                #
# =========================================================================== #

DAG_ID = "erco_carga_tariffs"
TABLA = "tariffs"
CONN_ID = "erco_postgres"
# Usuario que registra la carga en los datos de control (created_by / updated_by)
USUARIO_ETL = "kevin"
# Zona horaria del DAG: el schedule "0 8 * * *" se interpreta en hora de Colombia
ZONA_HORARIA = ZoneInfo("America/Bogota")
DATA_DIR = Path("/opt/airflow/data")
ARCHIVO = DATA_DIR / "tariffs 4.csv"                       # archivo que se carga
TMP_DIR = Path("/opt/airflow/tmp/erco")
EXTRAIDO = TMP_DIR / f"{DAG_ID}_extraido.parquet"          # lo escribe extraer, lo borra transformar
TRANSFORMADO = TMP_DIR / f"{DAG_ID}_transformado.parquet"  # lo escribe transformar, lo borra cargar

COMPONENTES = ["g", "t", "d", "r", "c", "p"]
COLUMNAS = ["id_market", "voltage_level", "cdi", *COMPONENTES, "cu"]
LLAVE = ["id_market", "voltage_level", "cdi"]

# Si la tarifa ya existe, solo se actualiza cuando algún componente cambió
# created_at lo pone la BD (DEFAULT CURRENT_TIMESTAMP); esta sentencia pone created_by al insertar
# y updated_at / updated_by solo cuando actualiza (al insertar quedan NULL)
SQL_UPSERT = """
    INSERT INTO public.tariffs (id_market, voltage_level, cdi, g, t, d, r, c, p, cu, source_file, created_by)
    VALUES (%(id_market)s, %(voltage_level)s, %(cdi)s, %(g)s, %(t)s, %(d)s, %(r)s,
            %(c)s, %(p)s, %(cu)s, %(source_file)s, %(usuario)s)
    ON CONFLICT (id_market, voltage_level, cdi) DO UPDATE SET
        g = EXCLUDED.g, t = EXCLUDED.t, d = EXCLUDED.d, r = EXCLUDED.r,
        c = EXCLUDED.c, p = EXCLUDED.p, cu = EXCLUDED.cu,
        source_file = EXCLUDED.source_file,
        updated_at  = CURRENT_TIMESTAMP,
        updated_by  = EXCLUDED.created_by
    WHERE (tariffs.g, tariffs.t, tariffs.d, tariffs.r, tariffs.c, tariffs.p, tariffs.cu)
          IS DISTINCT FROM (EXCLUDED.g, EXCLUDED.t, EXCLUDED.d, EXCLUDED.r, EXCLUDED.c, EXCLUDED.p, EXCLUDED.cu)
"""

log = logging.getLogger(__name__)


# =========================================================================== #
# TAREA 1: EXTRAER                                                             #
# =========================================================================== #

def extraer():
    """Lee el archivo como texto, valida el encabezado y guarda extraido.parquet."""
    if not ARCHIVO.is_file():
        raise FileNotFoundError(f"No existe el archivo: {ARCHIVO}")

    df = pd.read_csv(ARCHIVO, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    df.columns = [c.strip().lower() for c in df.columns]
    if list(df.columns) != COLUMNAS:
        raise ValueError(f"{ARCHIVO.name}: encabezado {list(df.columns)}, se esperaba {COLUMNAS}")
    df["source_file"] = ARCHIVO.name
    log.info("Extraído %s: %s filas", ARCHIVO.name, len(df))

    TMP_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(EXTRAIDO, index=False)
    log.info("Guardado %s", EXTRAIDO)


# =========================================================================== #
# TAREA 2: TRANSFORMAR                                                         #
# =========================================================================== #

def transformar():
    """Tipa, aplica reglas de negocio, guarda transformado.parquet y borra extraido.parquet."""
    df = pd.read_parquet(EXTRAIDO)

    # Texto sin espacios sobrantes; las celdas vacías quedan como nulo
    for col in COLUMNAS:
        df[col] = df[col].str.strip().replace("", pd.NA)
    for col in ["id_market", "voltage_level", "cdi"]:
        df[col] = pd.to_numeric(df[col]).astype("Int32")
    for col in COMPONENTES + ["cu"]:
        df[col] = pd.to_numeric(df[col])

    obligatorias = ["id_market", "voltage_level", *COMPONENTES, "cu"]
    vacios = df[obligatorias].isna().sum()
    if vacios.any():
        raise ValueError(f"Columnas obligatorias con valores vacíos: {vacios[vacios > 0].to_dict()}")

    # Niveles 2 y 3: el cdi no aplica
    cdi_sobrante = df["voltage_level"].isin([2, 3]) & df["cdi"].notna()
    if cdi_sobrante.any():
        log.warning("%s tarifas de nivel 2/3 traían cdi; se deja en NULL", int(cdi_sobrante.sum()))
        df.loc[cdi_sobrante, "cdi"] = pd.NA

    # CU debe ser la suma de los componentes
    diferencia = (df[COMPONENTES].sum(axis=1) - df["cu"]).abs()
    no_cuadra = df[diferencia > 0.01]
    if not no_cuadra.empty:
        log.warning("%s tarifas donde CU != G+T+D+R+C+P: %s",
                    len(no_cuadra), no_cuadra[LLAVE].head().to_dict("records"))

    repetidas = df.duplicated(subset=LLAVE, keep="last")
    if repetidas.any():
        log.warning("%s filas con %s repetido; se conserva la última", int(repetidas.sum()), LLAVE)
        df = df[~repetidas]

    df.to_parquet(TRANSFORMADO, index=False)
    log.info("Guardado %s (%s filas)", TRANSFORMADO, len(df))
    EXTRAIDO.unlink()
    log.info("Borrado %s", EXTRAIDO)


# =========================================================================== #
# TAREA 3: CARGAR                                                              #
# =========================================================================== #

def cargar():
    """UPSERT de transformado.parquet en la tabla y borra transformado.parquet."""
    df = pd.read_parquet(TRANSFORMADO)
    df["usuario"] = USUARIO_ETL  # created_by / updated_by

    # NaN/NaT -> None para que PostgreSQL reciba NULL
    df = df.astype(object)
    df = df.where(pd.notna(df), None)
    filas = df.to_dict("records")

    conn = PostgresHook(postgres_conn_id=CONN_ID).get_conn()
    try:
        with conn.cursor() as cur:
            cur.executemany(SQL_UPSERT, filas)
            afectadas = cur.rowcount  # insertadas + actualizadas; las que no cambiaron no cuentan
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    log.info("%s: %s filas | %s insertadas o actualizadas | %s sin cambios",
             TABLA, len(filas), afectadas, len(filas) - afectadas)
    TRANSFORMADO.unlink()
    log.info("Borrado %s", TRANSFORMADO)


# =========================================================================== #
# DEFINICIÓN DEL DAG                                                           #
# =========================================================================== #

default_args = {
    "owner": USUARIO_ETL,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "do_xcom_push": False,  # los datos pasan entre tareas por parquet, no por XCom
}

with DAG(
    dag_id=DAG_ID,
    description="Carga diaria de tariffs 4.csv a la tabla tariffs",
    default_args=default_args,
    start_date=datetime(2026, 11, 1, tzinfo=ZONA_HORARIA),
    schedule="0 8 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["erco", "carga"],
    doc_md=__doc__,
) as dag:

    tarea_extraer = PythonOperator(
        task_id="extraer",
        python_callable=extraer,
    )

    tarea_transformar = PythonOperator(
        task_id="transformar",
        python_callable=transformar,
    )

    tarea_cargar = PythonOperator(
        task_id="cargar",
        python_callable=cargar,
    )

    tarea_extraer >> tarea_transformar >> tarea_cargar
