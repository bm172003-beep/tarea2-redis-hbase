#!/usr/bin/env python3
"""Parte C — Preguntas P1 a P4 y P6 (P5 está en p5_salting.py). Cada una imprime la consulta, el resultado, el
tiempo medido (mediana de 5 corridas, la primera aparte) y deja los tiempos en resultados/tiempos.json.

    python preguntas.py --todas
    python preguntas.py p1 p3
"""
import argparse
import json

from t2 import consultas as q
from t2 import datos
from t2.config import RESULTADOS, TABLA_LECTURAS, TABLA_TIPO
from t2.hbase_util import conectar as hbase, tamano_tabla_kb
from t2.medir import guardar, hardware, medir
from t2.redis_util import conectar as redis


def titulo(t):
    print(f"\n{'=' * 100}\n{t}\n{'=' * 100}", flush=True)


def fmt_ms(x):
    return f"{x:,.1f} ms" if x < 1000 else f"{x / 1000:,.2f} s"


def p1(r, conn):
    titulo("P1 · Ahora — Redis contra HBase (último valor de las 16 estaciones y las 5 con más flujo)")
    tipos = q.catalogo(r)
    ests = sorted(tipos)
    (ult_r, top_r), t_r, c_r = medir(lambda: q.p1_redis(r, ests))
    (ult_h, top_h), t_h, c_h = medir(lambda: q.p1_hbase(conn, ests, tipos))
    metodo = {v[2] for v in ult_h.values() if v}
    (_, n_ing), t_i, c_i = medir(lambda: q.p1_hbase_ingenuo(conn, "CC-001"), corridas=3)
    guardar("P1", "redis (16 HGETALL + ZREVRANGE, 1 viaje)", t_r, c_r)
    guardar("P1", f"hbase (16 scans en reversa, limit 1) [{', '.join(sorted(metodo))}]", t_h, c_h)
    guardar("P1", "hbase ingenuo (recorrer toda 1 estación, ×16 habría que repetirlo)", t_i, c_i, {"filas_recorridas": n_ing})

    print("Último valor por estación (por MEDICIÓN):")
    for e in ests:
        ts = ult_r[e]["ts"]
        print(f"  {e:8s} {tipos[e]:26s} redis={ult_r[e]['valor']:8.2f}   hbase={ult_h[e][1]:8.2f}  {'✓' if ult_h[e][1] == ult_r[e]['valor'] else '✗ DISTINTO'}")
    print("Top 5 flujo vehicular — Redis :", [(e, round(v, 2)) for e, v in top_r])
    print("Top 5 flujo vehicular — HBase :", [(e, round(v, 2)) for e, v in top_h])
    print(f"\nTiempos (mediana de 5, sin la 1ª):  Redis {fmt_ms(t_r)}  ·  HBase {fmt_ms(t_h)} ({t_h / t_r:,.0f}× más lento)  ·  "
          f"HBase ingenuo, UNA estación: {fmt_ms(t_i)} ({n_ing:,} filas recorridas)")

    print("\nÚltimo por MEDICIÓN contra último por LLEGADA:")
    ll_r, ll_h = q.llegada_redis(r, ests), q.llegada_hbase(conn, ests)
    dif = q.p1_diferencias(ult_r, ll_r)
    for d in dif:
        e = d["estacion"]
        print(f"  Estación que cambia de respuesta: {e}")
        print(f"    por medición: ts_ms={d['ultimo_por_medicion']['ts_ms']} valor={d['ultimo_por_medicion']['valor']}")
        print(f"    por llegada : timestamp={d['ultimo_por_llegada']['timestamp']} valor={d['ultimo_por_llegada']['valor']} (entrada {d['ultimo_por_llegada']['id_stream']})")
        hh = ll_h[e]
        print(f"    (HBase, columna m:llegada, la confirma: entrada {'-'.join(map(str, hh[0]))} → {hh[1]} valor {hh[2]})" if hh else "    (HBase: sin m:llegada)")
    if not dif:
        print("  ninguna estación cambia de respuesta (¿corrió la hora en vivo completa?)")
    print("\nInterpretación: un dashboard usaría Redis: la respuesta ya está calculada en memoria (un viaje); en HBase 'lo último' no sale "
          "gratis con la clave estacion#timestamp y hay que pagar un scan en reversa por estación.")
    (RESULTADOS / "p1.json").write_text(json.dumps({"top5_redis": top_r, "top5_hbase": top_h, "diferencias_medicion_vs_llegada": dif}, indent=1, ensure_ascii=False), encoding="utf-8")


def p2(r, conn):
    titulo("P2 · Ventana de 10 minutos — Redis (leyendo ventana:*)")
    (ancla, medias), t, c = medir(lambda: q.p2(r))
    guardar("P2", "redis (ZRANGEBYSCORE ventana:* ×8, 1 viaje)", t, c)
    print(f"Ancla = medición más reciente del stream: ts_ms={ancla} → ventana [{ancla - 600000}, {ancla}]")
    for e, (m, n) in medias.items():
        print(f"  {e:8s} promedio de flujo = {m:7.2f}   ({n} lecturas en la ventana)")
    print(f"Tiempo (mediana de 5): {fmt_ms(t)}")
    # memoria: ventana recortada contra la misma ventana SIN recortar
    est = "CC-001"
    n_con, usado = r.zcard(f"ventana:{est}"), r.memory_usage(f"ventana:{est}")
    tmp = f"tmp:ventana_sin_recorte:{est}"
    r.delete(tmp)
    _, vivo = datos.generar()
    pipe = r.pipeline()
    for ev in vivo:
        if ev["estacion_id"] == est:
            ms = datos.ts_ms(ev["timestamp"])
            pipe.zadd(tmp, {f"{ms}:{ev['valor']}": ms})
    pipe.execute()
    n_sin, usado_sin = r.zcard(tmp), r.memory_usage(tmp)
    r.delete(tmp)
    por_entrada = usado_sin / n_sin
    por_dia = 172_800 / 16
    print(f"\nMEMORY USAGE ventana:{est}: {usado:,} bytes con {n_con} entradas (recortada a 10 min) · sin recortar la hora en vivo: {usado_sin:,} bytes con {n_sin} entradas "
          f"(~{por_entrada:,.0f} bytes por entrada)")
    print(f"Sin recorte crece sin parar: a ~{por_dia:,.0f} lecturas por estación al día serían ~{por_entrada * por_dia / 2**20:,.1f} MB por estación por día "
          f"(~{por_entrada * por_dia * 365 / 2**30:,.2f} GB por estación en un año), por 8 estaciones, en RAM.")
    print("Por qué un sorted set con el timestamp como puntaje: ZRANGEBYSCORE entrega 'los últimos 10 minutos' en O(log N + M) sin recorrer todo, "
          "ZREMRANGEBYSCORE recorta lo viejo con la misma lógica, y el miembro (ts:valor) hace que un evento repetido no se duplique.")


def p3(r, conn):
    titulo("P3 · Un rango — HBase: CC-001, 04-nov, [08:00, 10:00)")
    res_a, t_a, c_a = medir(lambda: q.p3_rango(conn))
    res_b, t_b, c_b = medir(lambda: q.p3_completo(conn))
    guardar("P3", "scan por rango (row_start/row_stop)", t_a, c_a, {"filas": res_a[0]})
    guardar("P3", "recorrido de la tabla entera + filtro", t_b, c_b, {"filas": res_b[0]})
    n, cuartos = res_a
    print(f"Lecturas en el rango: {n}   (mismo resultado con el recorrido completo: {'✓' if res_a == res_b else '✗ DISTINTO ' + str(res_b[0])})")
    for qq, m in cuartos.items():
        print(f"  {qq}  promedio {m:7.2f}")
    print(f"\nScan por rango: {fmt_ms(t_a)}  ·  tabla entera + filtro: {fmt_ms(t_b)}  →  {t_b / t_a:,.0f}× más lento")
    print(f"Con cinco AÑOS en vez de cinco días (×{365}): el scan por rango sigue leyendo las mismas ~{n} filas (casi igual: ~{fmt_ms(t_a)}); "
          f"el recorrido completo leería ×365 filas → del orden de {fmt_ms(t_b * 365)} (extrapolación lineal, no medida; en un cluster se reparte entre regiones).")


def p4(r, conn):
    titulo("P4 · Dos patrones de acceso — PM2.5 entre 07:00 y 09:00 del 05-nov (Redis catálogo + HBase)")
    ra, t_a, c_a = medir(lambda: q.p4_a(r, conn))
    rb, t_b, c_b = medir(lambda: q.p4_b(conn))
    guardar("P4", "(a) lecturas: un rango por estación (catálogo en Redis)", t_a, c_a)
    guardar("P4", "(b) lecturas_tipo: un solo scan", t_b, c_b)
    igual = ra.keys() == rb.keys() and all(abs(ra[e][0] - rb[e][0]) < 1e-9 and ra[e][1] == rb[e][1] and ra[e][2] == rb[e][2] for e in ra)
    print(f"{'estación':10s} {'promedio':>9s} {'máximo':>8s}  hora del máximo")
    for e, (m, mx, ts) in ra.items():
        print(f"{e:10s} {m:9.2f} {mx:8.2f}  {ts}")
    print(f"\nLas dos tablas dan lo mismo: {'✓' if igual else '✗ DISTINTO'}")
    print(f"(a) {len(ra)} rangos en `lecturas`: {fmt_ms(t_a)}   ·   (b) 1 scan en `lecturas_tipo`: {fmt_ms(t_b)}   →   (a)/(b) = {t_a / t_b:,.1f}×")
    k1, k2 = tamano_tabla_kb(TABLA_LECTURAS), tamano_tabla_kb(TABLA_TIPO)
    carga = RESULTADOS / "carga_historica.json"
    puts = 864_000
    print(f"\nLo que cuesta tener la segunda tabla:")
    print(f"  · escrituras: cada evento se escribe 2 veces → {2 * (puts + 7_200):,} put en vez de {puts + 7_200:,} (el doble)")
    print(f"  · disco: lecturas {k1:,} KB + lecturas_tipo {k2:,} KB = {k1 + k2:,} KB: la segunda tabla pesa {100 * k2 / k1:.0f} % de la primera "
          f"({100 * (k2 / k1 - 1):.0f} % más) y el disco total se multiplica por {(k1 + k2) / k1:.2f}. Pesa más porque su row key es más larga y HBase repite la clave en cada celda")
    print("  · consistencia: HBase no tiene transacciones entre tablas. Si el put a la segunda falla después de la primera, quedan desalineadas; "
          "por eso la ingesta no hace XACK hasta escribir las dos, y reintenta el lote completo (idempotente).")
    print("  · cuándo no valdría la pena: si la consulta por tipo es rara o tolera lentitud, si la escritura es el cuello de botella, o si hay pocas "
          "estaciones por tipo (un rango por estación ya es casi un scan).")


def p6(r, conn):
    titulo("P6 · La propia — ¿qué tan inusual es el flujo de ahora frente a la historia propia de cada estación? (Redis + HBase)")
    res, t, c = medir(lambda: q.p6(r, conn), corridas=3)
    guardar("P6", "redis (ranking) + hbase (5 estaciones × 5 días)", t, c)
    for d in res:
        print(f"  {d['estacion']:8s} ahora={d['valor_ahora']:7.2f} · {d['lecturas_historicas']:,} lecturas históricas · percentil {d['percentil']:5.1f} · "
              f"superado {d['veces_superado']:,} veces · última vez: {d['ultima_vez_superado']}")
    print(f"Tiempo (mediana de 3): {fmt_ms(t)}")
    print("Reparto: Redis decide el QUÉ (las 5 estaciones con más flujo y su valor de ahora: ranking:flujo, que HBase no tiene a mano); HBase decide el CUÁNDO "
          "(cinco días de lecturas por estación: Redis solo guarda 10 minutos). Ninguno solo puede responderla.")


PREGUNTAS = {"p1": p1, "p2": p2, "p3": p3, "p4": p4, "p6": p6}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("preguntas", nargs="*", choices=sorted(PREGUNTAS))
    ap.add_argument("--todas", action="store_true")
    a = ap.parse_args()
    elegidas = sorted(PREGUNTAS) if a.todas or not a.preguntas else a.preguntas
    r, conn = redis(), hbase()
    print("Computador:", hardware())
    for k in elegidas:
        PREGUNTAS[k](r, conn)
    print("\nTiempos acumulados en resultados/tiempos.json")


if __name__ == "__main__":
    main()
