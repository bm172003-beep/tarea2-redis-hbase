#!/usr/bin/env python3
"""Deja Redis listo para una corrida limpia: borra lo que dejó una anterior y crea los dos grupos desde el inicio."""
from t2.config import STREAM
from t2.redis_util import conectar, crear_grupos, limpiar

r = conectar()
n = limpiar(r)
crear_grupos(r)
print(f"Redis listo: {n} claves de corridas anteriores borradas · stream {STREAM} con los grupos ingesta y tablero")
