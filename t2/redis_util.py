"""Redis: conexión, grupos de consumidores y limpieza de las claves de la tarea."""
import redis

from t2.config import GRUPO_INGESTA, GRUPO_TABLERO, REDIS_HOST, REDIS_PORT, STREAM


def conectar():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    r.ping()
    return r


def limpiar(r):
    """Borra el stream (con sus grupos) y todo lo que el pipeline deja en Redis."""
    patrones = ["sensores:t2", "ultimo:*", "ranking:*", "ventana:*", "tipo:*", "t2:*"]
    n = 0
    for p in patrones:
        for k in r.scan_iter(p):
            n += r.delete(k)
    return n


def crear_grupos(r):
    """Los DOS grupos parten del principio del stream (id "0") y se crean ANTES de que el productor
    escriba: cada grupo ve todo el stream, sin importar cuándo arranque su consumidor."""
    for g in (GRUPO_INGESTA, GRUPO_TABLERO):
        try:
            r.xgroup_create(STREAM, g, id="0", mkstream=True)
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise


def info_grupo(r, nombre):
    for g in r.xinfo_groups(STREAM):
        if g["name"] == nombre:
            return g
    return None


def esperar_enviados(r, n, timeout=180):
    """Espera a que el productor lleve al menos n eventos enviados (así el consumidor encuentra lotes completos)."""
    import time
    from t2.config import CLAVE_ENVIADOS
    fin = time.time() + timeout
    while time.time() < fin:
        if int(r.get(CLAVE_ENVIADOS) or 0) >= n:
            return True
        time.sleep(0.2)
    return False
