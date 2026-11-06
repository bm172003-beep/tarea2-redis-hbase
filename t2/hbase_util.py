"""HBase: conexión, creación de tablas pre-divididas, escritura por lotes y conteo."""
import subprocess
import time

import happybase

from t2.config import (HBASE_HOST, HBASE_PORT, RAIZ, SPLITS_LECTURAS, SPLITS_TIPO, TABLA_LECTURAS, TABLA_TIPO)


def conectar(esperar_s=90):
    """Conexión Thrift (puerto 9090). Reintenta un rato: el servicio Thrift tarda en subir."""
    fin = time.time() + esperar_s
    while True:
        try:
            conn = happybase.Connection(HBASE_HOST, HBASE_PORT, timeout=120_000)
            conn.tables()
            return conn
        except Exception:
            if time.time() > fin:
                raise
            time.sleep(2)


def shell(comandos):
    """Ejecuta comandos en el `hbase shell` del contenedor y devuelve la salida útil."""
    p = subprocess.run(["docker", "compose", "exec", "-T", "hbase", "hbase", "shell", "-n"], cwd=RAIZ,
                       input=comandos, capture_output=True, text=True, encoding="utf-8", errors="replace")
    ruido = ("SLF4J", "hbase:", "Took ", "HBase Shell", 'Use "help"', "For Reference", "Version ")
    return "\n".join(l for l in (p.stdout + p.stderr).splitlines() if l.strip() and not l.startswith(ruido))


def _lista(splits):
    return "[" + ", ".join(f"'{s}'" for s in splits) + "]"


def crear_tablas(conn, forzar=False):
    """lecturas y lecturas_tipo, pre-divididas. VERSIONS => 3: así D.1 puede mostrar las versiones de una celda."""
    if forzar:
        for t in (TABLA_LECTURAS, TABLA_TIPO):
            if t.encode() in conn.tables():
                conn.delete_table(t, disable=True)
    existentes = conn.tables()
    cmds = []
    if TABLA_LECTURAS.encode() not in existentes:
        cmds.append(f"create '{TABLA_LECTURAS}', {{NAME => 'm', VERSIONS => 3}}, SPLITS => {_lista(SPLITS_LECTURAS)}")
    if TABLA_TIPO.encode() not in existentes:
        cmds.append(f"create '{TABLA_TIPO}', {{NAME => 'm', VERSIONS => 3}}, SPLITS => {_lista(SPLITS_TIPO)}")
    if cmds:
        print(shell("\n".join(cmds)))
    return conn


def regiones(conn, tabla):
    return [(r["start_key"].decode() or "(inicio)", r["end_key"].decode() or "(fin)") for r in conn.table(tabla).regions()]


def contar(conn, tabla):
    """Filas de la tabla (FirstKeyOnlyFilter + KeyOnlyFilter: trae solo la primera clave de cada fila)."""
    return sum(1 for _ in conn.table(tabla).scan(filter="FirstKeyOnlyFilter() AND KeyOnlyFilter()"))


def escribir(conn, tabla, pares, lote=2000):
    """put por LOTES (un viaje a HBase por lote, no por fila). `pares` = iterable de (clave, columnas)."""
    n = 0
    with conn.table(tabla).batch(batch_size=lote) as b:
        for clave, cols in pares:
            b.put(clave, cols)
            n += 1
    return n


# --- utilidades para los experimentos -------------------------------------------------------------
def borrar_vivo(conn):
    """Quita de las dos tablas las filas que escribe la hora en vivo (deja solo el histórico).
    Sirve para repetir los experimentos sin recargar los 864.000 eventos."""
    from t2 import datos
    _, vivo = datos.generar()
    for tabla, fclave in ((TABLA_LECTURAS, datos.clave_lecturas), (TABLA_TIPO, datos.clave_tipo)):
        with conn.table(tabla).batch(batch_size=2000) as b:
            for ev in vivo:
                b.delete(fclave(ev))


def escrituras_por_region(conn, tabla):
    """writeRequestCount de cada región de `tabla`, según las métricas JMX del propio HBase (Lab 07, 3.2).
    HBase las publica cada ~10 s: esperar antes de leer."""
    import json
    import urllib.request
    url = f"http://{HBASE_HOST}:16010/jmx?qry=Hadoop:service=HBase,name=RegionServer,sub=Regions"
    m = json.load(urllib.request.urlopen(url))["beans"][0]
    return {(r["start_key"].decode() or "(inicio)"):
            m.get(f"Namespace_default_table_{tabla}_region_{r['name'].decode().split('.')[-2]}_metric_writeRequestCount", 0)
            for r in conn.table(tabla).regions()}


def tamano_tabla_kb(tabla):
    """Espacio en disco de la tabla (KB): se vacía la memoria a disco (flush) y se mide la carpeta de sus HFiles."""
    shell(f"flush '{tabla}'")
    p = subprocess.run(["docker", "compose", "exec", "-T", "hbase", "du", "-sk", f"/data/hbase/data/default/{tabla}"],
                       cwd=RAIZ, capture_output=True, text=True)
    return int(p.stdout.split()[0])
