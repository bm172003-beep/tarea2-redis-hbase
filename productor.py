#!/usr/bin/env python3
"""B.2 — Productor: lee la hora en vivo (7.200 eventos) y la escribe en el stream `sensores:t2` con XADD, a 20×.

    python productor.py                 # los 7.200 eventos, uno cada 25 ms (~3 min)
    python productor.py --n 3000        # solo los primeros 3.000 (para el experimento D.2)

XADD con MAXLEN ~ (recorte aproximado): ver t2/config.py y el README.
"""
import argparse
import time

from t2 import datos
from t2.config import CLAVE_ENVIADOS, CLAVE_FIN, MAXLEN, N_VIVO, PASO_VIVO_S, STREAM
from t2.redis_util import conectar, crear_grupos


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=N_VIVO, help="cuántos eventos enviar (por defecto, todos)")
    ap.add_argument("--paso", type=float, default=PASO_VIVO_S, help="segundos entre eventos (0,025 = 20×)")
    ap.add_argument("--desde", type=int, default=0, help="índice del primer evento (para reanudar tras un corte, D.2)")
    a = ap.parse_args()

    r = conectar()
    r.delete(CLAVE_FIN)                               # "fin" vuelve a significar "aún no termino". Va ANTES de generar los eventos (~5 s):
    crear_grupos(r)                                   # una señal vieja (el AOF la conserva tras un reinicio) haría terminar a un consumidor
    _, vivo = datos.generar()                         # que arranque mientras tanto
    vivo = vivo[a.desde: a.desde + a.n]
    print(f"productor: {len(vivo):,} eventos -> {STREAM} (un evento cada {a.paso * 1000:.0f} ms, MAXLEN ~ {MAXLEN:,})", flush=True)

    inicio = time.monotonic()
    for i, ev in enumerate(vivo):
        r.xadd(STREAM, ev, maxlen=MAXLEN, approximate=True)
        if (i + 1) % 500 == 0:
            r.set(CLAVE_ENVIADOS, a.desde + i + 1)
            print(f"  {a.desde + i + 1:,} enviados", flush=True)
        time.sleep(max(0, inicio + (i + 1) * a.paso - time.monotonic()))     # sin deriva acumulada
    r.set(CLAVE_ENVIADOS, a.desde + len(vivo))
    r.set(CLAVE_FIN, a.desde + len(vivo))                       # los consumidores terminan cuando ven esto y no les queda nada
    print(f"productor: listo, {len(vivo):,} enviados en {time.monotonic() - inicio:.1f} s", flush=True)


if __name__ == "__main__":
    main()