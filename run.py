#!/usr/bin/env python3
"""Un solo comando: corre el pipeline de punta a punta sobre una máquina limpia (con Docker arriba).

    python run.py                  # (Windows)   ·   ./run.sh   (Linux / Mac)

Pasos: ambiente virtual + dependencias → docker compose up -d --wait --build → carga histórica a HBase →
Redis limpio con los dos grupos → ingesta + tablero + productor (la hora en vivo, ~3 min) → verificar.py.

    python run.py --sin-vivo       # solo infraestructura + carga histórica
    python run.py --sin-preguntas  # sin las preguntas P1–P6 (más rápido)
    python run.py --experimentos   # además, los experimentos de falla D.1 y D.2 (modifican el estado: van al final)
"""
import argparse
import json
import os
import subprocess
import sys
import time
import venv
from pathlib import Path

# Forzar codificación UTF-8 en Windows para evitar fallos por cp1252
os.environ["PYTHONUTF8"] = "1"
os.environ["PYTHONIOENCODING"] = "utf-8"

RAIZ = Path(__file__).resolve().parent
VENV = RAIZ / ".venv"
LOGS = RAIZ / "resultados" / "logs"
tiempos = {}


def py():
    return str(VENV / ("Scripts/python.exe" if os.name == "nt" else "bin/python"))


def paso(nombre):
    print(f"\n=== {nombre}", flush=True)
    return time.perf_counter()


def fin(nombre, t0):
    tiempos[nombre] = round(time.perf_counter() - t0, 1)
    print(f"    {nombre}: {tiempos[nombre]} s", flush=True)


def correr(*args, check=True):
    env = os.environ.copy()
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    return subprocess.run([py(), *args], cwd=RAIZ, check=check, env=env)


def ambiente():
    t0 = paso("1. Ambiente virtual y dependencias")
    if not Path(py()).exists():
        venv.EnvBuilder(with_pip=True).create(VENV)
    ya = subprocess.run([py(), "-c", "import redis, happybase, pandas, psutil"], cwd=RAIZ, capture_output=True).returncode == 0
    if not ya:
        subprocess.run([py(), "-m", "pip", "install", "-q", "-r", "requirements.txt"], cwd=RAIZ, check=True)
    fin("ambiente", t0)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sin-vivo", action="store_true", help="solo infraestructura y carga histórica")
    ap.add_argument("--sin-preguntas", action="store_true", help="no corre las preguntas P1–P6")
    ap.add_argument("--experimentos", action="store_true", help="corre también D.1 y D.2 (tardan varios minutos)")
    a = ap.parse_args()
    inicio = time.perf_counter()

    ambiente()

    t0 = paso("2. Contenedores: docker compose up -d --wait --build (la primera vez construye HBase, ~340 MB)")
    subprocess.run(["docker", "compose", "up", "-d", "--wait", "--build"], cwd=RAIZ, check=True)
    fin("compose_up", t0)

    t0 = paso("3. Carga histórica a HBase (864.000 eventos, por lotes)")
    correr("carga_historica.py")
    fin("carga_historica", t0)

    if not a.sin_vivo:
        t0 = paso("4. Redis limpio, con los grupos ingesta y tablero desde el inicio del stream")
        correr("preparar.py")
        fin("preparar_redis", t0)

        t0 = paso("5. El camino en vivo: dos consumidores independientes + el productor (7.200 eventos a 20×)")
        LOGS.mkdir(parents=True, exist_ok=True)
        consumidores = []
        env = os.environ.copy()
        env["PYTHONUTF8"] = "1"
        env["PYTHONIOENCODING"] = "utf-8"
        for script in ("ingesta.py", "tablero.py"):
            log = open(LOGS / f"{script[:-3]}.log", "w", encoding="utf-8")
            consumidores.append((script, subprocess.Popen([py(), script], cwd=RAIZ, stdout=log, stderr=subprocess.STDOUT, env=env)))
        time.sleep(4)                                   # que ambos estén conectados antes de producir
        correr("productor.py")
        for script, p in consumidores:
            try:
                p.wait(timeout=240)
                print(f"    {script}: terminó con código {p.returncode} (log en resultados/logs/)")
            except subprocess.TimeoutExpired:
                p.kill()
                print(f"    {script}: no terminó a tiempo, se detuvo (ver resultados/logs/)")
        fin("camino_en_vivo", t0)

        t0 = paso("6. Verificación contra los números de control")
        r = correr("verificar.py", check=False)
        fin("verificacion", t0)
        tiempos["verificacion_ok"] = r.returncode == 0

        if not a.sin_preguntas:
            t0 = paso("7. Preguntas P1–P4 y P6 (mediana de 5 corridas; tarda unos minutos por los recorridos completos de P3)")
            correr("preguntas.py", "--todas")
            fin("preguntas", t0)
            t0 = paso("8. P5: hotspotting y salting (tres tablas, métricas JMX por región)")
            correr("p5_salting.py")
            fin("p5_salting", t0)

        if a.experimentos:
            for nombre, args in (("D.1 consumidor muere entre put y XACK", ["experimento_d1.py"]),
                                 ("D.1 contraste: XACK antes del put", ["experimento_d1.py", "--ack-antes"]),
                                 ("D.2 Redis se reinicia con AOF (la elegida)", ["experimento_d2.py", "aof"]),
                                 ("D.2 Redis se reinicia sin persistencia (la contraria)", ["experimento_d2.py", "ninguna"]),
                                 ("D.2 costo de appendfsync", ["experimento_d2.py", "fsync"])):
                t0 = paso(nombre)
                correr(*args, check=False)
                fin(nombre, t0)

    tiempos["total_s"] = round(time.perf_counter() - inicio, 1)
    (RAIZ / "resultados").mkdir(exist_ok=True)
    (RAIZ / "resultados" / "run_tiempos.json").write_text(json.dumps(tiempos, indent=1), encoding="utf-8")
    print(f"\nListo en {tiempos['total_s'] / 60:.1f} min. Tiempos en resultados/run_tiempos.json")


if __name__ == "__main__":
    main()
