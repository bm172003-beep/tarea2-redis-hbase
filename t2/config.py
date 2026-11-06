"""Constantes de la Tarea 2: los parámetros del enunciado, en un solo lugar."""
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
RESULTADOS = RAIZ / "resultados"

# --- los datos (enunciado, sección 2) ---
SEMILLA = 2026                                   # OJO: el generador trae 2509 por defecto
TZ_CL = timezone(timedelta(hours=-3))
T0 = datetime(2026, 11, 2, 0, 0, tzinfo=TZ_CL)   # 2026-11-02T00:00:00-03:00
N_HIST = 864_000                                 # 5 días × 172.800
N_VIVO = 7_200                                   # una hora
PASO_VIVO_S = 0.025                              # 20× el real: un evento cada 25 ms

COMUNAS = {"CC": "Concepción", "TAL": "Talcahuano", "HUA": "Hualpén", "SPP": "San Pedro de la Paz",
           "CHI": "Chiguayante", "PEN": "Penco", "COR": "Coronel"}

# --- números de control (eventos, repetidos, claves distintas) ---
CONTROL = {"historico": (864_000, 17_090, 846_906), "vivo": (7_200, 153, 7_047), "total": (871_200, 17_243, 853_953)}

# --- Redis ---
REDIS_HOST = os.environ.get("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
STREAM = "sensores:t2"
GRUPO_INGESTA = "ingesta"
GRUPO_TABLERO = "tablero"
# MAXLEN ~ : el stream guarda como mucho ~10.000 entradas. A 20× (40 ev/s) son 250 s de atraso que
# un consumidor puede acumular sin perder nada; al ritmo real (2 ev/s), ~83 min. Pesa ~2 MB.
# Más allá, Redis recorta las entradas más viejas AUNQUE nadie las haya leído (ver README).
MAXLEN = 10_000
VENTANA_MS = 10 * 60 * 1000                      # el tablero conserva 10 minutos de medición
CLAVE_FIN = "t2:productor:fin"                   # el productor la escribe al terminar
CLAVE_ENVIADOS = "t2:productor:enviados"
CLAVE_TS_MAX = "t2:tablero:ts_max"               # la medición más reciente vista (ancla de la ventana)

# --- HBase ---
HBASE_HOST = os.environ.get("HBASE_HOST", "127.0.0.1")
HBASE_PORT = int(os.environ.get("HBASE_PORT", 9090))
TABLA_LECTURAS = "lecturas"            # row key: estacion#timestamp
TABLA_TIPO = "lecturas_tipo"           # row key: tipo#timestamp#estacion
# Cortes de regiones (justificados en el README): cada región recibe ~25 % de las filas.
SPLITS_LECTURAS = ["CC-005", "HUA-001", "SPP-002"]
SPLITS_TIPO = ["flujo_vehicular", "flujo_vehicular#2026-11-04T12", "ocupacion_estacionamiento"]
