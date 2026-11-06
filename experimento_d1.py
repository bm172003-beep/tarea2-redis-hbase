#!/usr/bin/env python3
"""D.1 — El consumidor muere a mitad de lote: después del put y antes del XACK.

    python experimento_d1.py              # el experimento
    python experimento_d1.py --ack-antes  # CONTRASTE: el orden incorrecto (XACK antes del put): se pierden eventos

Parte de HBase con SOLO el histórico (borra las filas de la hora en vivo), corre productor + tablero + ingesta-1 (que
muere de verdad con os._exit(1)), muestra XPENDING, deja que ingesta-2 recupere el lote y comprueba que HBase termina
con exactamente 853.953 filas por tabla, sin duplicados, y qué quedó en las celdas (versiones).
"""
import argparse
import json
import os
import subprocess
import sys
import time

from t2 import datos
from t2.config import CONTROL, GRUPO_INGESTA, RAIZ, RESULTADOS, STREAM, TABLA_LECTURAS, TABLA_TIPO
from t2.hbase_util import borrar_vivo, conectar as hbase, contar
from t2.redis_util import conectar as redis, esperar_enviados

PY = sys.executable
LOGS = RESULTADOS / "logs"

def lanzar(log, *args):
    LOGS.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    f = open(LOGS / log, "w", encoding="utf-8")
    return subprocess.Popen([PY, *args], cwd=RAIZ, stdout=f, stderr=subprocess.STDOUT, env=env)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ack-antes", action="store_true")
    a = ap.parse_args()
    r, conn = redis(), hbase()
    esperado_hist, esperado_total = CONTROL["historico"][2], CONTROL["total"][2]
    informe = {"modo": "contraste: XACK antes del put" if a.ack_antes else "XACK después del put"}

    print("1) Estado de partida: HBase solo con el histórico")
    borrar_vivo(conn)
    base = [contar(conn, t) for t in (TABLA_LECTURAS, TABLA_TIPO)]
    print(f"   filas: lecturas={base[0]:,} lecturas_tipo={base[1]:,} (histórico = {esperado_hist:,})")
    subprocess.run([PY, "preparar.py"], cwd=RAIZ, check=True)

    print("2) Productor (rápido, 2 ms entre eventos) + tablero + ingesta-1, que muere tras el lote 3")
    prod = lanzar("d1_productor.log", "productor.py", "--paso", "0.002")
    tab = lanzar("d1_tablero.log", "tablero.py")
    esperar_enviados(r, 1000)                          # que ya haya un colchón en el stream: así ingesta-1 lee lotes completos de 200
    args = ["ingesta.py", "--consumidor", "ingesta-1", "--morir-tras-lote", "3"] + (["--ack-antes-del-put"] if a.ack_antes else [])
    ing1 = lanzar("d1_ingesta1.log", *args)
    ing1.wait(timeout=180)
    print(f"   ingesta-1 terminó con código {ing1.returncode} (1 = murió de verdad con os._exit(1), sin pasar por el XACK)")
    informe["codigo_salida_ingesta1"] = ing1.returncode

    print("3) Qué quedó en XPENDING (grupo ingesta)")
    resumen = r.xpending(STREAM, GRUPO_INGESTA)
    print(f"   entradas pendientes: {resumen['pending']} · rango de IDs: {resumen['min']} … {resumen['max']}")
    print(f"   a nombre de: {resumen['consumers']}")
    pendientes = r.xpending_range(STREAM, GRUPO_INGESTA, min="-", max="+", count=max(resumen["pending"], 1)) if resumen["pending"] else []
    claves = []
    for p in pendientes:
        for _id, c in r.xrange(STREAM, p["message_id"], p["message_id"]):
            claves.append((datos.clave_lecturas(c), datos.clave_tipo(c), p["message_id"]))
    informe["pendientes"] = {"cantidad": resumen["pending"], "consumidores": resumen["consumers"]}
    if pendientes:
        primeras = [(x["message_id"], x["consumer"], "entregada %d vez" % x["times_delivered"]) for x in pendientes[:3]]
        print(f"   primeras entradas: {primeras}")

    print("4) ingesta-2 recupera las pendientes (XAUTOCLAIM) y termina el trabajo")
    time.sleep(3)                                      # las pendientes deben llevar ≥ 2 s quietas para poder reclamarlas
    ing2 = lanzar("d1_ingesta2.log", "ingesta.py", "--consumidor", "ingesta-2")
    for p in (prod, tab, ing2):
        p.wait(timeout=300)
    print((LOGS / "d1_ingesta2.log").read_text(encoding="utf-8").strip())

    print("5) Resultado")
    pend_final = r.xpending(STREAM, GRUPO_INGESTA)["pending"]
    filas = [contar(conn, t) for t in (TABLA_LECTURAS, TABLA_TIPO)]
    print(f"   XPENDING al final: {pend_final} (debe ser 0)")
    print(f"   filas finales: lecturas={filas[0]:,} lecturas_tipo={filas[1]:,} · control del enunciado: {esperado_total:,} → "
          f"{'EXACTO: ni faltan ni sobran filas' if filas == [esperado_total] * 2 else f'DIFERENCIA {filas[0] - esperado_total:+,} / {filas[1] - esperado_total:+,}'}")
    informe.update({"xpending_final": pend_final, "filas_finales": filas, "esperado": esperado_total})

    if claves:
        tabla = conn.table(TABLA_LECTURAS)
        con_2 = sum(1 for k, _, _ in claves if len(tabla.cells(k, b"m:valor", versions=3)) >= 2)
        print(f"   de las {len(claves)} entradas recuperadas, {con_2} filas tienen 2 versiones en la celda (el put original y el reproceso)")
        for k, _, i in claves[:3]:
            celdas = tabla.cells(k, b"m:valor", versions=3, include_timestamp=True)
            print(f"   fila {k.decode()}  (entrada {i}): " + " | ".join(f"valor={v.decode()} @ {ts}" for v, ts in celdas))
        print("   → el reproceso escribe la MISMA clave con el MISMO valor: HBase agrega una versión de la celda (o la sobrescribe) y la "
              "cantidad de filas no cambia. La clave sale de la medición, no de la llegada: por eso reprocesar no duplica filas.")
        informe["recuperadas_con_2_versiones"] = con_2
    elif a.ack_antes:
        faltan = esperado_total - filas[0]
        print(f"   CONTRASTE: con XACK antes del put, XPENDING es 0 pero FALTAN ~{faltan} filas: esas entradas se confirmaron sin haberse escrito y "
              "ya nadie las reclama. Por eso el XACK va después del put.")
        informe["filas_perdidas"] = faltan

    print("6) verificar.py")
    env = os.environ.copy()
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    v = subprocess.run([PY, "verificar.py"], cwd=RAIZ, env=env)
    informe["verificar_ok"] = v.returncode == 0
    nombre = "d1_contraste.json" if a.ack_antes else "d1.json"
    RESULTADOS.mkdir(exist_ok=True)
    (RESULTADOS / nombre).write_text(json.dumps(informe, indent=1, ensure_ascii=False, default=str), encoding="utf-8")


if __name__ == "__main__":
    main()
