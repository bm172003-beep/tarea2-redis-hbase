#!/usr/bin/env python3
"""D.2 — Redis se reinicia con el pipeline a medio camino (~3.000 eventos producidos).

    python experimento_d2.py aof                 # la configuración elegida (Parte A): AOF, appendfsync everysec
    python experimento_d2.py ninguna             # la contraria: sin persistencia
    python experimento_d2.py aof --abrupto       # en vez de `docker compose restart` (que apaga con gracia), un SIGKILL al contenedor
    python experimento_d2.py fsync               # costo de appendfsync: sin persistencia · everysec · always
    python experimento_d2.py perdida             # cuántas entradas se pierden con un SIGKILL justo tras escribir (3 repeticiones por configuración)

Para cada configuración muestra qué sobrevivió al reinicio (stream, grupos y pendientes, ultimo:*, ventanas), cuál era la
ventana de pérdida (eventos en Redis y todavía no en HBase) y si, reanudando, HBase termina con los 853.953.
Al terminar deja Redis con su configuración normal (AOF).
"""
import argparse
import json
import os
import subprocess
import sys
import time

import redis as redis_lib

from t2.config import CLAVE_FIN, CLAVE_TS_MAX, CONTROL, GRUPO_INGESTA, GRUPO_TABLERO, RAIZ, RESULTADOS, STREAM, TABLA_LECTURAS, TABLA_TIPO, N_VIVO
from t2.hbase_util import borrar_vivo, conectar as hbase, contar
from t2.redis_util import conectar as redis_conectar, esperar_enviados

PY = sys.executable
LOGS = RESULTADOS / "logs"
CONFIG = {"aof": {"REDIS_APPENDONLY": "yes", "REDIS_APPENDFSYNC": "everysec"},
          "ninguna": {"REDIS_APPENDONLY": "no"}}


def lanzar(log, *args):
    LOGS.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    f = open(LOGS / log, "w", encoding="utf-8")
    return subprocess.Popen([PY, *args], cwd=RAIZ, stdout=f, stderr=subprocess.STDOUT, env=env)


def compose(*args, env=None):
    subprocess.run(["docker", "compose", *args], cwd=RAIZ, check=True, env={**os.environ, **(env or {})}, capture_output=True)


def levantar_redis(env):
    """Recrea el contenedor con esa configuración (el volumen se conserva) y espera a que responda."""
    compose("up", "-d", "--force-recreate", "--wait", "redis", env=env)


def esperar_redis(seg=60):
    fin = time.time() + seg
    while time.time() < fin:
        try:
            return redis_conectar()
        except Exception:
            time.sleep(0.5)
    raise RuntimeError("Redis no volvió a responder")


def instantanea(r):
    s = {"stream_xlen": r.xlen(STREAM) if r.exists(STREAM) else 0, "grupos": {}}
    if r.exists(STREAM):
        for g in r.xinfo_groups(STREAM):
            s["grupos"][g["name"]] = {"pendientes": g["pending"], "lag": g.get("lag"), "leidas": g.get("entries-read"), "consumidores": g["consumers"]}
        if GRUPO_INGESTA in s["grupos"] and s["grupos"][GRUPO_INGESTA]["pendientes"]:
            s["pendientes_por_consumidor"] = r.xpending(STREAM, GRUPO_INGESTA)["consumers"]
    s["ultimo_claves"] = len(list(r.scan_iter("ultimo:*")))
    s["ventanas"] = {k: r.zcard(k) for k in sorted(r.scan_iter("ventana:*"))}
    s["ranking_flujo"] = r.zcard("ranking:flujo")
    s["ts_max"] = r.get(CLAVE_TS_MAX)
    return s


def resumen_linea(s):
    g = s["grupos"]
    return (f"stream {s['stream_xlen']:,} entradas · grupos {sorted(g) or 'NINGUNO'} · pendientes ingesta "
            f"{g.get(GRUPO_INGESTA, {}).get('pendientes', 0)} · lag ingesta {g.get(GRUPO_INGESTA, {}).get('lag')} · "
            f"ultimo:* {s['ultimo_claves']}/16 · ventanas {len(s['ventanas'])}/8 ({sum(s['ventanas'].values())} lecturas) · ts_max {s['ts_max']}")


def fsync():
    print("Costo de appendfsync: 5.000 XADD seguidos (uno por viaje) con cada configuración")
    filas = {}
    for nombre, env in (("sin persistencia", {"REDIS_APPENDONLY": "no"}), ("AOF everysec", {"REDIS_APPENDONLY": "yes", "REDIS_APPENDFSYNC": "everysec"}),
                        ("AOF always", {"REDIS_APPENDONLY": "yes", "REDIS_APPENDFSYNC": "always"})):
        levantar_redis(env)
        r = esperar_redis()
        r.delete("t2:prueba_fsync")
        t0 = time.perf_counter()
        for i in range(5000):
            r.xadd("t2:prueba_fsync", {"i": i}, maxlen=1000, approximate=True)
        seg = time.perf_counter() - t0
        r.delete("t2:prueba_fsync")
        filas[nombre] = {"xadd_por_s": round(5000 / seg), "ms_por_xadd": round(1000 * seg / 5000, 3)}
        print(f"  {nombre:18s}: {5000 / seg:9,.0f} XADD/s  ({1000 * seg / 5000:.3f} ms por XADD)")
    print("El pipeline produce 40 eventos/s a 20× (2/s al ritmo real): incluso 'always' sobra. Elegir por la pérdida aceptable, no por la velocidad.")
    levantar_redis({})
    (RESULTADOS / "d2_fsync.json").write_text(json.dumps(filas, indent=1, ensure_ascii=False), encoding="utf-8")


def perdida(repeticiones):
    """Muerte brusca (SIGKILL) INMEDIATAMENTE después de la última escritura: cuántas entradas ya confirmadas por Redis se pierden."""
    print(f"Pérdida ante una muerte brusca: 3.000 XADD seguidos y SIGKILL al contenedor justo después ({repeticiones} repeticiones por configuración)")
    filas = {}
    for nombre, env in (("sin persistencia", {"REDIS_APPENDONLY": "no"}), ("AOF everysec", {"REDIS_APPENDONLY": "yes", "REDIS_APPENDFSYNC": "everysec"}),
                        ("AOF always", {"REDIS_APPENDONLY": "yes", "REDIS_APPENDFSYNC": "always"})):
        levantar_redis(env)
        perdidas = []
        for _ in range(repeticiones):
            r = esperar_redis()
            r.delete("t2:prueba_perdida")
            for i in range(3000):
                r.xadd("t2:prueba_perdida", {"i": i})                       # cada XADD ya fue respondido: Redis lo dio por recibido
            subprocess.run(["docker", "kill", "pmd-t2-redis"], cwd=RAIZ, check=True, capture_output=True)
            compose("up", "-d", "--wait", "redis", env=env)
            r = esperar_redis()
            perdidas.append(3000 - (r.xlen("t2:prueba_perdida") if r.exists("t2:prueba_perdida") else 0))
        filas[nombre] = perdidas
        print(f"  {nombre:18s}: entradas perdidas en cada repetición = {perdidas}")
    print("Con everysec se puede perder lo escrito en el último segundo; con always, nada de lo ya respondido; sin persistencia, todo.")
    levantar_redis({})
    (RESULTADOS / "d2_perdida.json").write_text(json.dumps(filas, indent=1, ensure_ascii=False), encoding="utf-8")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("config", choices=["aof", "ninguna", "fsync", "perdida"])
    ap.add_argument("--repeticiones", type=int, default=3, help="solo para `perdida`")
    ap.add_argument("--abrupto", action="store_true", help="SIGKILL al contenedor en vez de `docker compose restart`")
    ap.add_argument("--n", type=int, default=3000, help="eventos producidos antes del reinicio")
    a = ap.parse_args()
    if a.config == "fsync":
        return fsync()
    if a.config == "perdida":
        return perdida(a.repeticiones)

    hb = hbase()
    esperado = CONTROL["total"][2]
    print(f"== D.2 · configuración: {a.config} · reinicio: {'SIGKILL (abrupto)' if a.abrupto else 'docker compose restart'} ==")
    levantar_redis(CONFIG[a.config])
    r = esperar_redis()
    borrar_vivo(hb)
    base = contar(hb, TABLA_LECTURAS)
    env_utf8 = {**os.environ, "PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8"}
    subprocess.run([PY, "preparar.py"], cwd=RAIZ, check=True, env=env_utf8)

    print(f"\n1) Se producen {a.n:,} eventos; el tablero los aplica; ingesta-1 muere tras el lote 2 dejando pendientes")
    tab = lanzar("d2_tablero.log", "tablero.py")
    prod = lanzar("d2_productor.log", "productor.py", "--n", str(a.n), "--paso", "0.002")
    esperar_enviados(r, 1000)                          # colchón en el stream: ingesta-1 lee lotes completos de 200
    ing = lanzar("d2_ingesta1.log", "ingesta.py", "--consumidor", "ingesta-1", "--morir-tras-lote", "2")
    for p in (prod, tab, ing):
        p.wait(timeout=300)
    antes = instantanea(r)
    en_hbase = contar(hb, TABLA_LECTURAS) - base
    ventana_perdida = antes["grupos"][GRUPO_INGESTA]["lag"]          # eventos que Redis tiene y ingesta aún no leyó
    print("   ANTES:", resumen_linea(antes))
    print(f"   en HBase (hora en vivo): {en_hbase:,} filas · en Redis y todavía NO en HBase: {ventana_perdida:,} eventos (el lag de ingesta)  ← la ventana de pérdida en este instante")

    print(f"\n2) Reinicio de Redis ({'docker kill' if a.abrupto else 'docker compose restart redis'})")
    if a.abrupto:
        subprocess.run(["docker", "kill", "pmd-t2-redis"], cwd=RAIZ, check=True, capture_output=True)
        compose("up", "-d", "--wait", "redis", env=CONFIG[a.config])
    else:
        compose("restart", "redis")
    r = esperar_redis()
    despues = instantanea(r)
    print("   DESPUÉS:", resumen_linea(despues))

    print("\n3) Qué sobrevivió")
    filas = [("stream", antes["stream_xlen"], despues["stream_xlen"]),
             ("grupos de consumidores", len(antes["grupos"]), len(despues["grupos"])),
             ("pendientes de ingesta (XPENDING)", antes["grupos"].get(GRUPO_INGESTA, {}).get("pendientes", 0), despues["grupos"].get(GRUPO_INGESTA, {}).get("pendientes", 0)),
             ("claves ultimo:*", antes["ultimo_claves"], despues["ultimo_claves"]),
             ("ventanas (lecturas)", sum(antes["ventanas"].values()), sum(despues["ventanas"].values()))]
    for nombre, x, y in filas:
        print(f"   {nombre:36s} antes {x:6,}  después {y:6,}   {'sobrevivió' if x == y else ('se PERDIÓ' if y == 0 else f'cambió ({y - x:+,})')}")

    print(f"\n4) Se reanuda: el productor envía el resto ({N_VIVO - a.n:,} eventos) y ingesta-2 y tablero-2 terminan")
    prod = lanzar("d2_productor2.log", "productor.py", "--desde", str(a.n), "--n", str(N_VIVO - a.n), "--paso", "0.002")
    fin_borrado = time.time() + 60                    # los consumidores solo arrancan cuando el productor ya borró la señal de fin vieja
    while r.exists(CLAVE_FIN) and time.time() < fin_borrado:
        time.sleep(0.1)
    ing2 = lanzar("d2_ingesta2.log", "ingesta.py", "--consumidor", "ingesta-2")
    tab2 = lanzar("d2_tablero2.log", "tablero.py", "--consumidor", "tablero-2")
    for p in (prod, ing2, tab2):
        p.wait(timeout=300)
    filas_fin = [contar(hb, t) for t in (TABLA_LECTURAS, TABLA_TIPO)]
    faltan = esperado - filas_fin[0]
    print(f"   filas finales en HBase: {filas_fin[0]:,} / {filas_fin[1]:,} · control: {esperado:,} → "
          f"{'TODO calza: no se perdió nada' if faltan == 0 else f'FALTAN {faltan:,} filas: eventos que estaban en Redis y no habían llegado a HBase cuando Redis se reinició'}")
    env_utf8 = {**os.environ, "PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8"}
    v = subprocess.run([PY, "verificar.py"], cwd=RAIZ, env=env_utf8)

    RESULTADOS.mkdir(exist_ok=True)
    (RESULTADOS / f"d2_{a.config}{'_abrupto' if a.abrupto else ''}.json").write_text(json.dumps(
        {"config": a.config, "abrupto": a.abrupto, "antes": antes, "despues": despues, "ventana_de_perdida_eventos": ventana_perdida,
         "filas_finales": filas_fin, "faltan": faltan, "verificar_ok": v.returncode == 0}, indent=1, ensure_ascii=False, default=str), encoding="utf-8")
    print("\nRedis vuelve a su configuración normal (AOF).")
    levantar_redis({})


if __name__ == "__main__":
    main()
