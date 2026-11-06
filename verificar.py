#!/usr/bin/env python3
"""B.3 — Verificación: compara lo que dejó el pipeline con los números de control del enunciado.

Imprime: eventos producidos · entradas procesadas y XPENDING de cada grupo (debe ser 0) · filas de cada
tabla de HBase · y el estado del tablero. Si algo no calza, lo dice y lo explica. Sale con código 1 si falla.
"""
import sys

from t2 import datos
from t2.config import (CLAVE_ENVIADOS, CLAVE_TS_MAX, CONTROL, GRUPO_INGESTA, GRUPO_TABLERO, STREAM, TABLA_LECTURAS,
                       TABLA_TIPO, VENTANA_MS)
from t2.hbase_util import conectar as conectar_hbase, contar
from t2.redis_util import conectar as conectar_redis, info_grupo

problemas = []


def ok(cond, msg_ok, msg_mal):
    print(("  ✓ " if cond else "  ✗ ") + (msg_ok if cond else msg_mal))
    if not cond:
        problemas.append(msg_mal)


def main():
    r, conn = conectar_redis(), conectar_hbase()
    enviados = int(r.get(CLAVE_ENVIADOS) or 0)
    historico, vivo = datos.generar()
    vivo_enviado = vivo[:enviados]
    claves_esperadas = {(e["estacion_id"], e["timestamp"]) for e in historico} | {(e["estacion_id"], e["timestamp"]) for e in vivo_enviado}
    esperado_filas = len(claves_esperadas)
    completo = enviados == len(vivo)

    print("== Eventos ==")
    print(f"  producidos (XADD): {enviados:,} de {len(vivo):,} · en el stream ahora (XLEN): {r.xlen(STREAM):,}")
    ok(enviados > 0, "el productor corrió", "el productor no escribió nada: ¿se ejecutó productor.py?")

    print("== Grupos de consumidores ==")
    for nombre in (GRUPO_INGESTA, GRUPO_TABLERO):
        g = info_grupo(r, nombre)
        if not g:
            ok(False, "", f"el grupo {nombre} no existe")
            continue
        print(f"  {nombre}: entradas leídas {g.get('entries-read')} · XPENDING {g['pending']} · lag {g.get('lag')} · consumidores {g['consumers']}")
        ok(g["pending"] == 0, f"{nombre}: XPENDING = 0 (todo confirmado)", f"{nombre}: quedan {g['pending']} entradas sin confirmar (XACK)")
        ok(g.get("entries-read") == enviados, f"{nombre}: leyó las {enviados:,} entradas producidas",
           f"{nombre}: leyó {g.get('entries-read')} de {enviados:,} producidas (¿recorte por MAXLEN o consumidor detenido?)")

    print("== HBase ==")
    print(f"  esperado: {esperado_filas:,} filas por tabla = histórico {CONTROL['historico'][2]:,} + vivo (claves nuevas)")
    for tabla in (TABLA_LECTURAS, TABLA_TIPO):
        n = contar(conn, tabla)
        ok(n == esperado_filas, f"{tabla}: {n:,} filas (calza)", f"{tabla}: {n:,} filas, se esperaban {esperado_filas:,} (diferencia {n - esperado_filas:+,})")
    if completo:
        print(f"  cuentas: {CONTROL['total'][0]:,} eventos − {CONTROL['total'][1]:,} repetidos tal cual − 4 choques de clave (misma estación y milisegundo, "
              f"distinto valor) = {CONTROL['total'][2]:,} claves distintas")
        ok(esperado_filas == CONTROL["total"][2], "las claves distintas calzan con el control del enunciado (853.953)",
           f"el generador dio {esperado_filas:,} claves, el enunciado dice {CONTROL['total'][2]:,}: ¿semilla distinta de 2026?")

    print("== Tablero (Redis) ==")
    ultimos = [k for k in r.scan_iter("ultimo:*")]
    ok(len(ultimos) == len({e["estacion_id"] for e in vivo_enviado}), f"{len(ultimos)} claves ultimo:*",
       f"hay {len(ultimos)} claves ultimo:*, se esperaban {len({e['estacion_id'] for e in vivo_enviado})}")
    # "último por MEDICIÓN": cada ultimo:<est> debe tener la medición más reciente de esa estación
    mejor = {}
    for e in vivo_enviado:
        ms = datos.ts_ms(e["timestamp"])
        if e["estacion_id"] not in mejor or ms >= mejor[e["estacion_id"]]:
            mejor[e["estacion_id"]] = ms
    malos = [k for k in ultimos if int(r.hget(k, "ts")) != mejor[k.split(":")[1]]]
    ok(not malos, "ultimo:* tiene, en todas, la medición más reciente (una lectura tardía no hizo retroceder el tablero)",
       f"{len(malos)} estaciones con un 'último' que no es la medición más reciente: {malos[:3]}")
    n_flujo = len({e["estacion_id"] for e in vivo_enviado if e["tipo_sensor"] == datos.FLUJO})
    ok(r.zcard("ranking:flujo") == n_flujo, f"ranking:flujo con {n_flujo} estaciones", f"ranking:flujo tiene {r.zcard('ranking:flujo')}, se esperaban {n_flujo}")
    ventanas = [k for k in r.scan_iter("ventana:*")]
    ts_max = int(r.get(CLAVE_TS_MAX) or 0)
    fuera = [k for k in ventanas if r.zcount(k, "-inf", f"({ts_max - VENTANA_MS}") > 0]
    ok(len(ventanas) == n_flujo and not fuera, f"{len(ventanas)} ventanas, todas dentro de los últimos 10 min de medición",
       f"ventanas: {len(ventanas)} (esperadas {n_flujo}); con datos fuera de los 10 min: {fuera[:3]}")
    tipos = {k: r.smembers(k) for k in r.scan_iter("tipo:*")}
    ok(len(tipos) == 3 and sum(len(v) for v in tipos.values()) == 16, "catálogo tipo:*: 3 tipos, 16 estaciones",
       f"catálogo incompleto: {({k: len(v) for k, v in tipos.items()})}")

    print()
    if problemas:
        print(f"✗ La verificación encontró {len(problemas)} problema(s). Si el desvío es esperable (p. ej. una corrida parcial o un experimento de falla), "
              "explíquenlo en el informe.")
        sys.exit(1)
    print("✓ VERIFICACIÓN OK: todo calza con los números de control.")


if __name__ == "__main__":
    main()
