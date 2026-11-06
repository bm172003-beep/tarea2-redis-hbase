#!/usr/bin/env python3
"""B.2 — Consumidor `ingesta`: baja el stream a HBase (las dos tablas) por lotes.

    python ingesta.py                              # consumidor ingesta-1
    python ingesta.py --consumidor ingesta-2       # otro consumidor del MISMO grupo
    python ingesta.py --morir-tras-lote 3          # D.1: muere después del put del lote 3 y ANTES del XACK

Regla de oro: el XACK va DESPUÉS del put en las DOS tablas. Si el proceso muere entre medio, el lote
queda en XPENDING y se reprocesa; como la row key sale de la medición, reprocesar no duplica filas.
"""
import argparse
import os
import time

from t2 import datos
from t2.config import (CLAVE_FIN, GRUPO_INGESTA, STREAM, TABLA_LECTURAS, TABLA_TIPO)
from t2.hbase_util import conectar
from t2.redis_util import conectar as conectar_redis, crear_grupos, info_grupo


def evento_de(campos):
    return {"estacion_id": campos["estacion_id"], "tipo_sensor": campos["tipo_sensor"],
            "valor": campos["valor"], "timestamp": campos["timestamp"]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--consumidor", default="ingesta-1")
    ap.add_argument("--lote", type=int, default=200)
    ap.add_argument("--idle-ms", type=int, default=2000, help="cuánto debe llevar quieta una entrada ajena para reclamarla")
    ap.add_argument("--morir-tras-lote", type=int, default=0, help="D.1: os._exit(1) tras el put del lote N, antes del XACK")
    ap.add_argument("--sin-recuperar", action="store_true", help="no reclama pendientes al arrancar")
    ap.add_argument("--ack-antes-del-put", action="store_true",
                    help="SOLO PARA EL CONTRASTE DE D.1: confirma (XACK) ANTES de escribir; con --morir-tras-lote muere entre medio")
    a = ap.parse_args()

    r = conectar_redis()
    crear_grupos(r)
    conn = conectar()
    t_lec, t_tipo = conn.table(TABLA_LECTURAS), conn.table(TABLA_TIPO)
    estado = {"lotes": 0, "procesadas": 0, "recuperadas": 0}

    def procesar(entradas, recuperadas=False):
        """put en las dos tablas y, solo después, XACK. `entradas` = [(id, campos), ...]"""
        validas = [(i, c) for i, c in entradas if c]          # una entrada recortada (sin contenido) no se puede escribir
        ids = [i for i, _ in entradas]
        estado["lotes"] += 1
        if a.ack_antes_del_put:                                 # ORDEN INCORRECTO, a propósito: confirma primero
            r.xack(STREAM, GRUPO_INGESTA, *ids)
            if a.morir_tras_lote and estado["lotes"] == a.morir_tras_lote:
                print(f"[{a.consumidor}] D.1 (contraste): muero DESPUÉS del XACK y ANTES del put ({len(ids)} entradas)", flush=True)
                os._exit(1)
        if validas:
            with t_lec.batch(batch_size=len(validas) + 1) as b1, t_tipo.batch(batch_size=len(validas) + 1) as b2:
                for i, c in validas:
                    ev = evento_de(c)
                    cols = datos.columnas(ev, llegada=i)
                    b1.put(datos.clave_lecturas(ev), cols)
                    b2.put(datos.clave_tipo(ev), cols)
            # ← aquí los datos YA están en HBase (las dos tablas); falta confirmar
        if not a.ack_antes_del_put:
            if a.morir_tras_lote and estado["lotes"] == a.morir_tras_lote:
                print(f"[{a.consumidor}] D.1: muero ahora, después del put y antes del XACK ({len(ids)} entradas)", flush=True)
                os._exit(1)
            r.xack(STREAM, GRUPO_INGESTA, *ids)
        estado["procesadas"] += len(ids)
        if recuperadas:
            estado["recuperadas"] += len(ids)

    def recuperar():
        """1) mis propias entradas pendientes (XREADGROUP con id 0) · 2) las de consumidores muertos (XAUTOCLAIM)."""
        while True:
            res = r.xreadgroup(GRUPO_INGESTA, a.consumidor, {STREAM: "0"}, count=a.lote)
            entradas = res[0][1] if res else []
            if not entradas:
                break
            print(f"[{a.consumidor}] recupero {len(entradas)} pendientes propias", flush=True)
            procesar(entradas, recuperadas=True)
        inicio = "0-0"
        while True:
            res = r.xautoclaim(STREAM, GRUPO_INGESTA, a.consumidor, min_idle_time=a.idle_ms, start_id=inicio, count=a.lote)
            siguiente, entradas = res[0], res[1]
            if entradas:
                print(f"[{a.consumidor}] reclamo {len(entradas)} entradas de otro consumidor (XAUTOCLAIM)", flush=True)
                procesar(entradas, recuperadas=True)
            if siguiente in ("0-0", b"0-0") or not entradas and siguiente == inicio:
                break
            inicio = siguiente

    print(f"[{a.consumidor}] ingesta lista (grupo {GRUPO_INGESTA}, lote {a.lote})", flush=True)
    if not a.sin_recuperar:
        recuperar()
    while True:
        res = r.xreadgroup(GRUPO_INGESTA, a.consumidor, {STREAM: ">"}, count=a.lote, block=1000)
        if res:
            procesar(res[0][1])
            continue
        # sin entradas nuevas: ¿terminó todo? (el productor avisó, y al grupo no le queda nada por entregar ni por confirmar)
        if not a.sin_recuperar:
            recuperar()
        g = info_grupo(r, GRUPO_INGESTA)
        if r.exists(CLAVE_FIN) and g and g.get("lag") in (0, None) and g.get("pending", 0) == 0:
            break
    print(f"[{a.consumidor}] listo: {estado['procesadas']:,} entradas procesadas ({estado['recuperadas']:,} recuperadas) en {estado['lotes']} lotes", flush=True)


if __name__ == "__main__":
    main()
