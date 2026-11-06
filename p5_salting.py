#!/usr/bin/env python3
"""P5 — Hotspotting y salting (HBase). La hora en vivo se escribe en TRES tablas con la misma información y distinta clave:

    A  estacion#ts          pre-dividida por estación (como `lecturas`)
    B  ts#estacion          pre-dividida por día
    C  salt#ts#estacion     salt = crc32(estacion) mod 4, pre-dividida por salt

Mide cuántas escrituras recibió cada región (métricas JMX de HBase, como en el Lab 07, 3.2) y el tiempo de leer todas las
lecturas de todas las estaciones entre 00:10 y 00:20 del 07-nov con cada diseño (16 rangos, 1 rango, 4 rangos).
"""
import json
import time
import zlib

from t2 import consultas as q
from t2 import datos
from t2.config import RESULTADOS
from t2.hbase_util import conectar, escribir, escrituras_por_region, shell
from t2.medir import guardar, hardware, medir

ESTACIONES = sorted(datos.TIPO_DE)


def salt(est):
    """crc32 (determinista; el hash() de Python cambia en cada ejecución) módulo 4."""
    return zlib.crc32(est.encode()) % 4


DISENOS = {
    "A estacion#ts": ("p5_estacion_ts", lambda ev: f"{ev['estacion_id']}#{ev['timestamp']}".encode(),
                      ["CC-005", "HUA-001", "SPP-002"]),
    "B ts#estacion": ("p5_ts_estacion", lambda ev: f"{ev['timestamp']}#{ev['estacion_id']}".encode(),
                      ["2026-11-03", "2026-11-04", "2026-11-05", "2026-11-06", "2026-11-07"]),
    "C salt#ts#estacion": ("p5_salt_ts_estacion", lambda ev: f"{salt(ev['estacion_id'])}#{ev['timestamp']}#{ev['estacion_id']}".encode(),
                           ["1", "2", "3"]),
}
INI, FIN = q.iso(2026, 11, 7, 0, 10), q.iso(2026, 11, 7, 0, 20)


def crear(conn, tabla, splits):
    if tabla.encode() in conn.tables():
        conn.delete_table(tabla, disable=True)
    lista = "[" + ", ".join(f"'{s}'" for s in splits) + "]"
    shell(f"create '{tabla}', {{NAME => 'm', VERSIONS => 1}}, SPLITS => {lista}")


def lecturas_a(conn, tabla):
    t = conn.table(tabla)
    return sum(1 for e in ESTACIONES for _ in t.scan(row_start=f"{e}#{INI}".encode(), row_stop=f"{e}#{FIN}".encode(), columns=[b"m:valor"]))


def lecturas_b(conn, tabla):
    return sum(1 for _ in conn.table(tabla).scan(row_start=INI.encode(), row_stop=FIN.encode(), columns=[b"m:valor"]))


def lecturas_c(conn, tabla):
    t = conn.table(tabla)
    return sum(1 for s in range(4) for _ in t.scan(row_start=f"{s}#{INI}".encode(), row_stop=f"{s}#{FIN}".encode(), columns=[b"m:valor"]))


def main():
    conn = conectar()
    _, vivo = datos.generar()
    print(f"Computador: {hardware()}")
    print(f"Estaciones por salt (crc32 mod 4): { {s: [e for e in ESTACIONES if salt(e) == s] for s in range(4)} }")
    for nombre, (tabla, _, splits) in DISENOS.items():
        crear(conn, tabla, splits)

    time.sleep(12)                                    # que las métricas ya incluyan la creación de las tablas
    antes = {n: escrituras_por_region(conn, t) for n, (t, _, _) in DISENOS.items()}
    for nombre, (tabla, fclave, _) in DISENOS.items():
        t0 = time.perf_counter()
        n = escribir(conn, tabla, ((fclave(ev), {b"m:valor": str(ev["valor"]).encode()}) for ev in vivo), lote=500)
        print(f"{nombre:22s}: {n:,} put en {time.perf_counter() - t0:.2f} s")
    print("Esperando a que HBase publique sus métricas (~10 s por ciclo)…")
    time.sleep(25)
    despues = {n: escrituras_por_region(conn, t) for n, (t, _, _) in DISENOS.items()}

    print("\nEscrituras por región (la tabla de 3 × regiones):")
    tabla_res = {}
    for nombre in DISENOS:
        deltas = {reg: despues[nombre][reg] - antes[nombre][reg] for reg in antes[nombre]}
        total = sum(deltas.values()) or 1
        tabla_res[nombre] = deltas
        print(f"\n  {nombre}  (total {sum(deltas.values()):,})   región por clave inicial → escrituras (% del total)")
        for reg, v in deltas.items():
            print(f"     {reg:>14s} → {v:6,d}  ({100 * v / total:5.1f} %)")
        mx, medio = max(deltas.values()), total / len(deltas)
        print(f"     región más cargada: {100 * mx / total:.1f} % del total · {mx / medio:.2f}× lo que recibiría con un reparto perfecto")

    print("\nLeer todas las lecturas de todas las estaciones entre 00:10 y 00:20 del 07-nov:")
    funciones = {"A estacion#ts": (lecturas_a, "16 rangos"), "B ts#estacion": (lecturas_b, "1 rango"), "C salt#ts#estacion": (lecturas_c, "4 rangos")}
    tiempos = {}
    for nombre, (fn, desc) in funciones.items():
        tabla = DISENOS[nombre][0]
        n, t, c = medir(lambda: fn(conn, tabla))
        tiempos[nombre] = (n, t)
        guardar("P5", f"{nombre} ({desc})", t, c, {"lecturas": n})
        print(f"  {nombre:22s} {desc:9s}: {n:,} lecturas en {t:7.1f} ms (mediana de 5)")
    iguales = len({n for n, _ in tiempos.values()}) == 1
    print(f"  mismas lecturas en los tres diseños: {'✓' if iguales else '✗ DISTINTO'}")

    (RESULTADOS / "p5.json").write_text(json.dumps({"escrituras_por_region": tabla_res,
                                                    "lectura_00_10_a_00_20": {k: {"lecturas": n, "mediana_ms": round(t, 2)} for k, (n, t) in tiempos.items()},
                                                    "estaciones_por_salt": {s: [e for e in ESTACIONES if salt(e) == s] for s in range(4)}},
                                                   indent=1, ensure_ascii=False), encoding="utf-8")
    print("\nDetalle en resultados/p5.json. La decisión (qué diseño elegir y bajo qué supuestos cambiaría) va en el informe, con estas cifras.")


if __name__ == "__main__":
    main()
