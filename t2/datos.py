"""Los eventos de la tarea y sus row keys. Todo sale de la semilla: no hay archivos de datos."""
import itertools
from datetime import datetime

from generador import ESTACIONES, eventos          # el generador del Lab 06, sin cambios

from t2.config import N_HIST, N_VIVO, SEMILLA, T0

TIPO_DE = dict(ESTACIONES)
FLUJO = "flujo_vehicular"


def generar():
    """(histórico, en vivo): los primeros 864.000 eventos y los 7.200 siguientes, en orden de LLEGADA."""
    todo = list(itertools.islice(eventos(T0, semilla=SEMILLA), N_HIST + N_VIVO))
    return todo[:N_HIST], todo[N_HIST:]


def ts_ms(iso):
    """Timestamp de MEDICIÓN en milisegundos desde 1970 (entero)."""
    return int(round(datetime.fromisoformat(iso).timestamp() * 1000))


# --- row keys ------------------------------------------------------------------------------------
# Salen de la estación y de la MEDICIÓN, nunca de la llegada: un evento repetido o tardío cae en la
# misma fila de siempre (idempotente). El timestamp ISO con milisegundos y offset -03:00 siempre
# mide 29 caracteres, así que ordenar como bytes es ordenar en el tiempo.
def clave_lecturas(ev):
    return f"{ev['estacion_id']}#{ev['timestamp']}".encode()


def clave_tipo(ev):
    # la estación va AL FINAL de la clave: dos estaciones del mismo tipo que midan en el mismo
    # milisegundo no se pisan
    return f"{ev['tipo_sensor']}#{ev['timestamp']}#{ev['estacion_id']}".encode()


def columnas(ev, llegada=None):
    c = {b"m:valor": str(ev["valor"]).encode(), b"m:tipo": ev["tipo_sensor"].encode()}
    if llegada:                                     # solo los eventos en vivo: ID de la entrada del stream
        c[b"m:llegada"] = llegada.encode()
    return c
