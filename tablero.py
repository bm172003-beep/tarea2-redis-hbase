#!/usr/bin/env python3
"""B.2 — Consumidor `tablero`: mantiene en Redis el estado de AHORA.

Al terminar la hora en vivo existen (revisables con redis-cli):
    ultimo:<estacion>    hash  valor, ts (medición, ms), tipo
    ranking:flujo        zset  estacion → último valor (solo flujo vehicular)
    ventana:<estacion>   zset  score = ts de medición (ms), solo flujo vehicular, últimos 10 min de medición
    tipo:<tipo>          set   las estaciones de cada tipo de sensor (el catálogo)

"Último" = último por MEDICIÓN: una lectura tardía no puede hacer retroceder el tablero. Se logra con un
script Lua que compara el ts del evento con el ts guardado y actualiza TODO (hash, ranking, ventana) de
forma atómica: o entra completo o no entra. Redis ejecuta un script sin intercalar otros comandos.
"""
import argparse

from t2 import datos
from t2.config import (CLAVE_FIN, CLAVE_TS_MAX, GRUPO_TABLERO, STREAM, VENTANA_MS)
from t2.redis_util import conectar, crear_grupos, info_grupo

LUA = """
-- KEYS: 1 ultimo:<est> · 2 ranking:flujo · 3 ventana:<est> · 4 ts_max · 5 tipo:<tipo>
-- ARGV: 1 est · 2 tipo · 3 valor · 4 ts_ms · 5 es_flujo (1/0) · 6 ventana_ms · 7 miembro de la ventana
local ts = tonumber(ARGV[4])
redis.call('SADD', KEYS[5], ARGV[1])                       -- catálogo: idempotente

local max = redis.call('GET', KEYS[4])                      -- la medición más reciente vista (ancla)
if (not max) or ts > tonumber(max) then
  redis.call('SET', KEYS[4], ARGV[4]); max = ts
else
  max = tonumber(max)
end

local actual = redis.call('HGET', KEYS[1], 'ts')
local nuevo = 0
if (not actual) or ts >= tonumber(actual) then              -- solo avanza (>=: ante un empate, gana el último en llegar, como HBase)
  redis.call('HSET', KEYS[1], 'valor', ARGV[3], 'ts', ARGV[4], 'tipo', ARGV[2])
  if ARGV[5] == '1' then redis.call('ZADD', KEYS[2], ARGV[3], ARGV[1]) end
  nuevo = 1
end

if ARGV[5] == '1' then                                      -- la ventana SÍ recibe las tardías: pertenecen a su minuto de medición
  redis.call('ZADD', KEYS[3], ts, ARGV[7])
  redis.call('ZREMRANGEBYSCORE', KEYS[3], '-inf', '(' .. string.format('%.0f', max - tonumber(ARGV[6])))
end
return nuevo
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--consumidor", default="tablero-1")
    ap.add_argument("--lote", type=int, default=200)
    a = ap.parse_args()

    r = conectar()
    crear_grupos(r)
    script = r.register_script(LUA)
    n = tardias = 0

    def aplicar(campos):
        nonlocal tardias
        est, tipo = campos["estacion_id"], campos["tipo_sensor"]
        flujo = tipo == datos.FLUJO
        ms = datos.ts_ms(campos["timestamp"])
        miembro = f"{ms}:{campos['valor']}"                 # un evento repetido produce el MISMO miembro: no se duplica
        avanzo = script(keys=[f"ultimo:{est}", "ranking:flujo", f"ventana:{est}", CLAVE_TS_MAX, f"tipo:{tipo}"],
                        args=[est, tipo, campos["valor"], ms, "1" if flujo else "0", VENTANA_MS, miembro])
        if not avanzo:
            tardias += 1                                    # lectura más vieja que la ya guardada: no mueve el "último"

    print(f"[{a.consumidor}] tablero listo (grupo {GRUPO_TABLERO})", flush=True)
    while True:
        res = r.xreadgroup(GRUPO_TABLERO, a.consumidor, {STREAM: ">"}, count=a.lote, block=1000)
        if res:
            entradas = [(i, c) for i, c in res[0][1] if c]
            for _, c in entradas:
                aplicar(c)
            r.xack(STREAM, GRUPO_TABLERO, *[i for i, _ in res[0][1]])
            n += len(entradas)
            continue
        g = info_grupo(r, GRUPO_TABLERO)
        if r.exists(CLAVE_FIN) and g and g.get("lag") in (0, None) and g.get("pending", 0) == 0:
            break
    # Recorte final: cada ventana se recorta cuando llega un evento de SU estación, con el ancla de ese momento;
    # al terminar, se recortan todas con la medición más reciente (el ancla final), para que ninguna conserve
    # entradas de más de 10 minutos de medición.
    ts_max = int(r.get(CLAVE_TS_MAX) or 0)
    for k in r.scan_iter("ventana:*"):
        r.zremrangebyscore(k, "-inf", f"({ts_max - VENTANA_MS}")
    print(f"[{a.consumidor}] listo: {n:,} eventos aplicados · {tardias:,} no movieron el 'último' (más viejos que el guardado)", flush=True)


if __name__ == "__main__":
    main()
