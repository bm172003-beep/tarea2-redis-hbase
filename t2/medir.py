"""Medición de tiempos: mediana de 5 corridas, descartando la primera (calienta cachés)."""
import json
import os
import platform
import statistics
import time

from t2.config import RESULTADOS


def medir(fn, corridas=5):
    """Ejecuta fn una vez aparte (calentamiento) y luego `corridas` veces. Devuelve (resultado, mediana_ms, todas_ms)."""
    fn()
    tiempos, res = [], None
    for _ in range(corridas):
        t0 = time.perf_counter()
        res = fn()
        tiempos.append((time.perf_counter() - t0) * 1000)
    return res, statistics.median(tiempos), [round(t, 2) for t in tiempos]


def hardware():
    info = {"sistema": platform.platform(), "procesador": platform.processor() or platform.machine(),
            "python": platform.python_version(), "cpus": os.cpu_count()}
    try:
        import psutil
        info["ram_gb"] = round(psutil.virtual_memory().total / 2**30, 1)
    except Exception:
        pass
    return info


def guardar(pregunta, variante, mediana_ms, todas_ms, extra=None):
    """Acumula las mediciones en resultados/tiempos.json (con el hardware, que el informe debe citar)."""
    ruta = RESULTADOS / "tiempos.json"
    datos = json.loads(ruta.read_text(encoding="utf-8")) if ruta.exists() else {"hardware": hardware(), "mediciones": {}}
    datos["hardware"] = hardware()
    datos["mediciones"].setdefault(pregunta, {})[variante] = {"mediana_ms": round(mediana_ms, 2), "corridas_ms": todas_ms, **(extra or {})}
    RESULTADOS.mkdir(exist_ok=True)
    ruta.write_text(json.dumps(datos, indent=1, ensure_ascii=False), encoding="utf-8")
