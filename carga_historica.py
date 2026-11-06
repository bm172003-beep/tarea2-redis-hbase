#!/usr/bin/env python3
"""B.1 — Carga histórica: los 864.000 eventos del histórico van DIRECTO a HBase (sin pasar por Redis).

    python carga_historica.py             # crea las tablas si faltan; si ya están completas, no las vuelve a cargar
    python carga_historica.py --forzar    # borra y recarga desde cero

Escribe por lotes (no put por put) en dos tablas y reporta tiempo y filas por segundo en
resultados/carga_historica.json.
"""
import argparse
import json
import platform
import time

from t2 import datos
from t2.config import CONTROL, RESULTADOS, TABLA_LECTURAS, TABLA_TIPO
from t2.hbase_util import conectar, contar, crear_tablas, escribir, regiones


def hardware():
    info = {"sistema": platform.platform(), "procesador": platform.processor() or platform.machine(), "python": platform.python_version()}
    try:
        import os, psutil
        info["cpus"] = os.cpu_count()
        info["ram_gb"] = round(psutil.virtual_memory().total / 2**30, 1)
    except Exception:
        pass
    return info


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--forzar", action="store_true", help="borra las tablas y recarga")
    a = ap.parse_args()
    esperado = CONTROL["historico"][2]                         # 846.906 claves distintas

    conn = conectar()
    crear_tablas(conn, forzar=a.forzar)
    if not a.forzar and all(contar(conn, t) >= esperado for t in (TABLA_LECTURAS, TABLA_TIPO)):
        print(f"Las tablas ya tienen el histórico completo ({esperado:,} filas): no se vuelve a cargar (usen --forzar para repetirlo)")
        return

    t0 = time.perf_counter()
    historico, _ = datos.generar()
    print(f"{len(historico):,} eventos del histórico generados en {time.perf_counter() - t0:.1f} s (semilla {datos.SEMILLA})")

    medidas = {}
    for tabla, fclave in ((TABLA_LECTURAS, datos.clave_lecturas), (TABLA_TIPO, datos.clave_tipo)):
        t1 = time.perf_counter()
        n = escribir(conn, tabla, ((fclave(ev), datos.columnas(ev)) for ev in historico))
        seg = time.perf_counter() - t1
        filas = contar(conn, tabla)
        medidas[tabla] = {"put": n, "segundos": round(seg, 1), "put_por_s": round(n / seg), "filas": filas,
                          "filas_esperadas": esperado, "regiones": regiones(conn, tabla)}
        print(f"{tabla}: {n:,} put en {seg:.1f} s ({n / seg:,.0f} put/s) → {filas:,} filas (esperado {esperado:,})")

    RESULTADOS.mkdir(exist_ok=True)
    (RESULTADOS / "carga_historica.json").write_text(json.dumps({"hardware": hardware(), "tablas": medidas}, indent=1, ensure_ascii=False), encoding="utf-8")
    print("detalle en resultados/carga_historica.json")


if __name__ == "__main__":
    main()
