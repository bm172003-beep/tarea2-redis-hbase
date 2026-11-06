"""Las preguntas de la Parte C. Cada función recibe el cliente de Redis (r) y/o de HBase (conn) y devuelve datos;
la medición de tiempos y la impresión están en preguntas.py."""
import statistics
from datetime import datetime

from t2 import datos
from t2.config import (CLAVE_TS_MAX, STREAM, TABLA_LECTURAS, TABLA_TIPO, TZ_CL, VENTANA_MS)


def iso(y, m, d, h=0, mi=0, s=0):
    """Timestamp con el MISMO formato que las row keys: 2026-11-04T08:00:00.000-03:00"""
    return datetime(y, m, d, h, mi, s, tzinfo=TZ_CL).isoformat(timespec="milliseconds")


def catalogo(r):
    """{estacion: tipo} desde el catálogo de Redis (tipo:<tipo>)."""
    out = {}
    for k in r.scan_iter("tipo:*"):
        for e in r.smembers(k):
            out[e] = k.split(":", 1)[1]
    return out


# ============================================================================================ P1
def p1_redis(r, ests):
    """Último valor de cada estación + top 5 de flujo, desde Redis: un solo viaje (pipeline)."""
    pipe = r.pipeline()
    for e in ests:
        pipe.hgetall(f"ultimo:{e}")
    pipe.zrevrange("ranking:flujo", 0, 4, withscores=True)
    res = pipe.execute()
    ultimos = {e: {"valor": float(h["valor"]), "ts": int(h["ts"]), "tipo": h["tipo"]} for e, h in zip(ests, res[:-1])}
    return ultimos, [(e, float(s)) for e, s in res[-1]]


def ultimo_hbase(conn, est):
    """Última lectura por MEDICIÓN de una estación en HBase: scan en REVERSA con limit=1. La clave `estacion#ts`
    ordena por tiempo; "$" (0x24) es mayor que "#" (0x23), así que f"{est}$" queda justo después de todas las filas
    de la estación y f"{est}#" justo antes. Si el scan en reversa no está disponible, cae a un scan hacia adelante
    sobre los últimos 10 minutos (documentado en el README)."""
    t = conn.table(TABLA_LECTURAS)
    try:
        for k, d in t.scan(row_start=f"{est}$".encode(), row_stop=f"{est}#".encode(), columns=[b"m:valor"], reverse=True, limit=1):
            return k.decode().split("#", 1)[1], float(d[b"m:valor"]), "reversa"
    except Exception:
        pass
    ultimo = None
    for k, d in t.scan(row_start=f"{est}#2026-11-07T00:50".encode(), row_stop=f"{est}$".encode(), columns=[b"m:valor"]):
        ultimo = (k.decode().split("#", 1)[1], float(d[b"m:valor"]), "hacia adelante (últimos 10 min)")
    return ultimo


def p1_hbase(conn, ests, tipos):
    ultimos = {e: ultimo_hbase(conn, e) for e in ests}
    flujo = sorted(((e, v[1]) for e, v in ultimos.items() if tipos[e] == datos.FLUJO), key=lambda x: -x[1])[:5]
    return ultimos, flujo


def p1_hbase_ingenuo(conn, est):
    """Lo que habría que hacer si no se pudiera leer en reversa: recorrer TODA la estación y quedarse con la última."""
    ultimo, n = None, 0
    for k, d in conn.table(TABLA_LECTURAS).scan(row_prefix=f"{est}#".encode(), columns=[b"m:valor"]):
        ultimo, n = k, n + 1
    return ultimo.decode(), n


def llegada_redis(r, ests):
    """Última lectura por LLEGADA: la primera aparición de cada estación leyendo el stream de atrás hacia adelante."""
    faltan, res = set(ests), {}
    for _id, c in r.xrevrange(STREAM, "+", "-", count=2000):
        if c["estacion_id"] in faltan:
            res[c["estacion_id"]] = {"id": _id, "timestamp": c["timestamp"], "valor": float(c["valor"])}
            faltan.discard(c["estacion_id"])
        if not faltan:
            break
    return res


def llegada_hbase(conn, ests):
    """Lo mismo desde HBase, con la columna m:llegada (ID de la entrada del stream) de las filas en vivo."""
    t, res = conn.table(TABLA_LECTURAS), {}
    for e in ests:
        mejor = None
        for k, d in t.scan(row_start=f"{e}#2026-11-07T00:50".encode(), row_stop=f"{e}$".encode(), columns=[b"m:valor", b"m:llegada"]):
            if b"m:llegada" in d:
                llegada = tuple(int(x) for x in d[b"m:llegada"].decode().split("-"))
                if mejor is None or llegada > mejor[0]:
                    mejor = (llegada, k.decode().split("#", 1)[1], float(d[b"m:valor"]))
        res[e] = mejor
    return res


def p1_diferencias(ultimos_medicion, llegada):
    """Estaciones donde 'último por medición' y 'último por llegada' son lecturas distintas, con las dos lecturas."""
    out = []
    for e, u in ultimos_medicion.items():
        ll = llegada[e]
        if datos.ts_ms(ll["timestamp"]) != u["ts"]:
            out.append({"estacion": e, "ultimo_por_medicion": {"ts_ms": u["ts"], "valor": u["valor"]},
                        "ultimo_por_llegada": {"timestamp": ll["timestamp"], "valor": ll["valor"], "id_stream": ll["id"]}})
    return out


# ============================================================================================ P2
def p2(r):
    """Promedio de flujo de los últimos 10 min de MEDICIÓN (anclados a la medición más reciente), leyendo ventana:*."""
    ancla = int(r.get(CLAVE_TS_MAX))
    ini = ancla - VENTANA_MS
    flujo = sorted(e for e, t in catalogo(r).items() if t == datos.FLUJO)
    pipe = r.pipeline()
    for e in flujo:
        pipe.zrangebyscore(f"ventana:{e}", ini, ancla)
    res = pipe.execute()
    medias = {e: (statistics.fmean(float(m.split(":")[1]) for m in miembros), len(miembros)) for e, miembros in zip(flujo, res) if miembros}
    return ancla, medias


# ============================================================================================ P3
def _cuarto(ts_iso):
    return f"{int(ts_iso[11:13]):02d}:{(int(ts_iso[14:16]) // 15) * 15:02d}"


def _resumen_cuartos(filas):
    """filas = iterable de (timestamp_iso, valor) → (cuántas, {cuarto: promedio})"""
    n, por = 0, {}
    for ts, v in filas:
        n += 1
        por.setdefault(_cuarto(ts), []).append(v)
    return n, {q: statistics.fmean(v) for q, v in sorted(por.items())}


def p3_rango(conn):
    """CC-001, 04-nov, [08:00, 10:00): scan por RANGO de row keys (va directo a las filas)."""
    ini, fin = f"CC-001#{iso(2026, 11, 4, 8)}", f"CC-001#{iso(2026, 11, 4, 10)}"
    filas = [(k.decode().split("#", 1)[1], float(d[b"m:valor"])) for k, d in
             conn.table(TABLA_LECTURAS).scan(row_start=ini.encode(), row_stop=fin.encode(), columns=[b"m:valor"])]
    return _resumen_cuartos(filas)


def p3_completo(conn):
    """Lo mismo recorriendo la tabla ENTERA con un filtro (RowFilter con regex): HBase lee todas las filas y descarta."""
    flt = "RowFilter(=, 'regexstring:^CC-001#2026-11-04T(08|09):')"
    filas = [(k.decode().split("#", 1)[1], float(d[b"m:valor"])) for k, d in
             conn.table(TABLA_LECTURAS).scan(filter=flt, columns=[b"m:valor"])]
    return _resumen_cuartos(filas)


# ============================================================================================ P4
def _stats_pm25(filas):
    """filas = iterable de (estacion, timestamp, valor), en orden de tiempo → {est: (promedio, máximo, hora_del_máximo)}.
    Si el máximo se repite, vale la primera ocurrencia (comparación estricta)."""
    acc = {}
    for est, ts, v in filas:
        a = acc.setdefault(est, {"suma": 0.0, "n": 0, "max": None, "ts_max": None})
        a["suma"] += v
        a["n"] += 1
        if a["max"] is None or v > a["max"]:
            a["max"], a["ts_max"] = v, ts
    return {e: (a["suma"] / a["n"], a["max"], a["ts_max"]) for e, a in sorted(acc.items())}


def p4_a(r, conn):
    """(a) En `lecturas` (estación primero): un rango por estación; la lista de estaciones sale del catálogo de Redis."""
    ests = sorted(r.smembers("tipo:calidad_aire"))
    t = conn.table(TABLA_LECTURAS)
    filas = []
    for e in ests:
        for k, d in t.scan(row_start=f"{e}#{iso(2026, 11, 5, 7)}".encode(), row_stop=f"{e}#{iso(2026, 11, 5, 9)}".encode(), columns=[b"m:valor"]):
            filas.append((e, k.decode().split("#", 1)[1], float(d[b"m:valor"])))
    return _stats_pm25(filas)


def p4_b(conn):
    """(b) En `lecturas_tipo` (tipo primero): UN solo scan; la clave es tipo#timestamp#estacion."""
    ini, fin = f"calidad_aire#{iso(2026, 11, 5, 7)}", f"calidad_aire#{iso(2026, 11, 5, 9)}"
    filas = []
    for k, d in conn.table(TABLA_TIPO).scan(row_start=ini.encode(), row_stop=fin.encode(), columns=[b"m:valor"]):
        _, ts, est = k.decode().split("#")
        filas.append((est, ts, float(d[b"m:valor"])))
    return _stats_pm25(filas)


# ============================================================================================ P6
def p6(r, conn, hasta="2026-11-07"):
    """La propia: ¿qué tan inusual es el flujo de AHORA de las 5 estaciones con más flujo, frente a su PROPIA historia?
    Redis decide el QUÉ (las 5 estaciones y su valor actual: ranking:flujo); HBase decide el CUÁNDO (cinco días de
    lecturas de cada una: en qué percentil cae ese valor, cuántas veces se superó y cuándo fue la última vez)."""
    top = r.zrevrange("ranking:flujo", 0, 4, withscores=True)
    t, out = conn.table(TABLA_LECTURAS), []
    for est, actual in top:
        actual = float(actual)
        n = mayores = 0
        ultima = None
        for k, d in t.scan(row_start=f"{est}#".encode(), row_stop=f"{est}#{hasta}".encode(), columns=[b"m:valor"]):
            n += 1
            if float(d[b"m:valor"]) > actual:
                mayores += 1
                ultima = k.decode().split("#", 1)[1]
        out.append({"estacion": est, "valor_ahora": actual, "lecturas_historicas": n,
                    "percentil": round(100 * (n - mayores) / n, 1), "veces_superado": mayores, "ultima_vez_superado": ultima})
    return out
