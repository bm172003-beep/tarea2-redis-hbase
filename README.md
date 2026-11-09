# Tarea 2 — Redis + HBase: del stream a la historia

Procesamiento Masivo de Datos · USM 2026-2 · Integrantes: Benjamín Medina, Javier Fernández

Pipeline completo: los eventos de 16 estaciones del Gran Concepción entran a un **stream de Redis**; un grupo de consumidores (`ingesta`) los baja a **HBase** sin perder ni duplicar nada; otro grupo (`tablero`) mantiene el estado de *ahora* en Redis. Redis guarda el presente (en memoria); HBase, la historia (en disco).

## 1. Reproducir desde cero

Requisitos: Docker (con `docker compose`, **≥ 4 GB de RAM asignados**, ~3 GB de disco) y Python 3.9 o más. Sin datos que descargar: todo se genera con la semilla 2026.

```bash
python run.py        # Windows          ·          ./run.sh      # Linux / Mac
```

Hace, en orden: ambiente virtual + `requirements.txt` → `docker compose up -d --wait --build` (la primera vez construye la imagen de HBase, ~340 MB, necesita red) → carga histórica → Redis limpio con los dos grupos → ingesta + tablero + productor (la hora en vivo dura ~3 min a 20×) → `verificar.py` → preguntas P1 a P4 y P6 → P5. Con `python run.py --experimentos` corre además D.1 y D.2 (modifican el estado de Redis y de HBase, por eso van al final y son opcionales). Tiempos de cada paso en `resultados/run_tiempos.json`.

**Cuánto tarda:** ~5,8 min (pipeline sin experimentos) / ~13,3 min (con `--experimentos`, 795,3 s medidos en `resultados/run_tiempos.json`). La primera vez suma ~2 min por la construcción de la imagen de HBase.
- **PC de prueba:** AMD Ryzen 5 (6 núcleos / 12 hilos), 15,9 GB RAM, Windows 11.

Antes de correr, apaguen lo que no se use: el Postgres de la Tarea 1 y los contenedores de los Labs 06 y 07 (`docker compose down` en sus carpetas). Los puertos 6379, 9090 y 16010 deben estar libres y HBase necesita memoria.

## 2. Qué hace cada archivo

| Archivo | Rol |
|---|---|
| `docker-compose.yml`, `Dockerfile` | Redis 7 y HBase (el del Lab 07), con healthchecks y volúmenes con nombre |
| `generador.py` | el del Lab 06, **sin cambios** (código, no datos) |
| `t2/` | configuración, claves, utilidades de Redis y HBase |
| `carga_historica.py` | B.1: 864.000 eventos a HBase, por lotes, a dos tablas pre-divididas |
| `productor.py` | B.2: la hora en vivo al stream `sensores:t2` con `XADD` a 20× |
| `ingesta.py` | B.2: grupo `ingesta`, stream → HBase; `XACK` después del put |
| `tablero.py` | B.2: grupo `tablero`, estado de ahora en Redis |
| `verificar.py` | B.3: compara con los números de control |
| `preguntas.py` | Parte C: P1, P2, P3, P4 y P6, con tiempos (mediana de 5, sin la 1ª) en `resultados/tiempos.json` |
| `p5_salting.py` | Parte C, P5: tres claves, escrituras por región (JMX) y lecturas de un rango |
| `experimento_d1.py` | Parte D.1: el consumidor muere entre el put y el XACK (y el contraste con el XACK antes del put) |
| `experimento_d2.py` | Parte D.2: Redis se reinicia con AOF y sin persistencia; `--abrupto` (SIGKILL); `fsync` (costo de appendfsync) |
| `run.py` / `run.sh` | un solo comando de punta a punta |

## 3. Los datos y los números de control

`eventos(t0 = 2026-11-02T00:00:00-03:00, semilla = 2026)`: los primeros 864.000 eventos son el histórico y los 7.200 siguientes, la hora en vivo. Con el generador sin modificar se reproducen **exactamente** los números del enunciado:

| | Histórico | En vivo | Total |
|---|---:|---:|---:|
| Eventos | 864.000 | 7.200 | 871.200 |
| Repetidos tal cual | 17.090 | 153 | 17.243 |
| Claves distintas (estación + timestamp) | 846.906 | 7.047 | 853.953 |

871.200 − 17.243 repetidos − 4 choques de clave (misma estación y milisegundo, distinto valor; HBase se queda con el último escrito) = 853.953. Cada tabla de HBase debe terminar con **853.953 filas**.

## 4. Decisiones

**Persistencia de Redis: AOF con `appendfsync everysec`, en un volumen con nombre** (escrito en el compose). El stream es lo único que separa a un evento de HBase: perderlo es perder historia, así que "ninguna" queda descartada. RDB guarda fotos cada cierto tiempo y pierde todo lo escrito desde la última. AOF con fsync por segundo pierde, como mucho, ~1 s de escrituras ante una muerte brusca, y a cambio sobreviven el stream, los grupos y sus pendientes, y el tablero. La configuración contraria (D.2) se levanta sin editar el compose: `REDIS_APPENDONLY=no docker compose up -d --force-recreate redis`.

**Row keys.** Salen de la estación y de la **medición**, nunca de la llegada: un evento repetido o tardío cae en su fila de siempre, de modo que reprocesar es *idempotente*.

| Tabla | Row key | La hace rápida | La hace lenta |
|---|---|---|---|
| `lecturas` | `estacion#timestamp` | una estación en un rango de tiempo (un scan) | un tipo de sensor (hay que visitar muchas estaciones) y "la última lectura" (el scan va hacia adelante: hay que leer en reversa o recorrer el rango) |
| `lecturas_tipo` | `tipo#timestamp#estacion` | un tipo de sensor en un rango de tiempo (un scan) | una sola estación (hay que filtrar). La estación va al final de la clave para que dos estaciones del mismo tipo con igual milisegundo no se pisen |

El timestamp ISO con milisegundos y offset `-03:00` mide siempre 29 caracteres, así que ordenar como bytes es ordenar en el tiempo. Familia `m` con `VERSIONS => 3`: reescribir una celda (D.1) deja versiones visibles del mismo valor.

**Regiones (SPLITS).** Cada región recibe ~25 % de las filas. Medido sobre el histórico:

| Tabla | Cortes | Filas por región | máx/mín |
|---|---|---|---:|
| `lecturas` | `CC-005`, `HUA-001`, `SPP-002` (4 estaciones por región) | 211.991 · 212.036 · 211.420 · 211.459 | 1,00 |
| `lecturas_tipo` | `flujo_vehicular`, `flujo_vehicular#2026-11-04T12`, `ocupacion_estacionamiento` | 211.941 · 212.260 · 211.440 · 211.265 | 1,00 |

Los cortes del Lab 07 (`COR`, `PEN`, `TAL`) dejaban las regiones de `lecturas` en 318.466 · 211.122 · 158.751 · 158.567 (máx/mín 2,01). En `lecturas_tipo`, flujo vehicular es el 50 % de las filas (8 de las 16 estaciones), por eso su prefijo se parte en dos por fecha.

**Stream: `MAXLEN ~ 10.000`.** Pesa ~2 MB. A 20× (40 eventos/s) el consumidor tolera ~250 s de atraso; al ritmo real (2/s), ~83 min. Pasado ese atraso, Redis recorta las entradas más viejas **aunque nadie las haya leído**: esos eventos no llegan a HBase (el `lag` del grupo lo delata, y `XPENDING` conserva el ID pero la entrada ya no tiene contenido). La hora en vivo (7.200) cabe entera: en la corrida normal no se recorta nada. `~` deja que Redis recorte por bloques enteros (más barato que exacto).

**Ingesta: `XACK` después del put.** El lote se escribe en las **dos** tablas y solo entonces se confirma. Si el proceso muere entre medio (D.1) o falla la segunda tabla, el lote queda en `XPENDING` y se reprocesa; como las claves son idempotentes, no hay duplicados. Un consumidor nuevo recupera pendientes con `XREADGROUP … id 0` (las propias) o `XAUTOCLAIM` (las de otro consumidor muerto).

**Tablero: último por *medición*.** Cada evento se aplica con un script Lua atómico que compara el `ts` de medición con el guardado y solo avanza si es mayor o igual; actualiza hash, ranking y ventana de una vez, y no se pueden intercalar otros comandos. Una lectura tardía (≈3,4 % de los eventos) no hace retroceder `ultimo:*` ni `ranking:flujo`, pero **sí** entra en `ventana:*`: pertenece a su minuto de medición. La ventana guarda 10 minutos anclados a la medición más reciente vista (`t2:tablero:ts_max`), y al terminar se recortan todas con el ancla final. El miembro de la ventana es `ts:valor`: un evento repetido es el mismo miembro y no se duplica.

**Preguntas (Parte C).** P1 (Redis contra HBase; el "último" en HBase sale con un scan en reversa de 1 fila por estación y, si la reversa no estuviera disponible, con un scan hacia adelante de los últimos 10 min), P2 (promedio sobre `ventana:*`), P3 (rango contra recorrido completo con filtro), P4 (`lecturas` contra `lecturas_tipo`, y su costo), P5 (`p5_salting.py`) y **P6, la propia**: *¿qué tan inusual es el flujo de ahora de las 5 estaciones con más flujo frente a su propia historia?* Redis decide el **qué** (las 5 estaciones y su valor de ahora: `ranking:flujo`); HBase decide el **cuándo** (cinco días de lecturas por estación: percentil de ese valor, cuántas veces se superó y la última vez). Ninguno la responde solo: Redis guarda 10 minutos; HBase no tiene a mano el ranking de ahora. "Historia" = lo *medido* antes de las 00:00 del 07-nov (incluye 5 lecturas tardías que llegaron durante la hora en vivo pero se midieron antes de medianoche).

**P5.** Tres tablas con la misma hora en vivo: `estacion#ts` (pre-dividida por estación, como `lecturas`), `ts#estacion` (pre-dividida por día) y `salt#ts#estacion` con `salt = crc32(estacion) mod 4` (se usa `crc32` y no `hash()` porque el de Python cambia en cada ejecución). Con 16 estaciones el salt no reparte parejo (4/5/4/3 estaciones por bucket).

## 5. Estado del trabajo

- [x] Infraestructura, carga histórica, productor, dos consumidores, verificación, `run.py` — corrido en la máquina del equipo (5,8 min, `VERIFICACIÓN OK`)
- [x] Preguntas P1 a P6 — corridas en la máquina del equipo; tiempos y hardware en `resultados/tiempos.json`, P1 a P6 en `resultados/p1.json`, `p5.json`
- [x] D.1 (y su contraste con el XACK antes del put) y D.2 (AOF, sin persistencia, SIGKILL, costo de fsync) — salidas en `resultados/d1*.json`, `d2_*.json`
- [x] Informe (5 páginas) en `informe/` (Word editable y PDF)
- [x] Volver a correr `python experimento_d2.py aof` tras la corrección del consumidor reanudado (ver sección 6), y, opcional, `python experimento_d2.py perdida`

## Cómo se corre cada parte

```bash
python run.py                       # pipeline + verificación + preguntas + P5 (un solo comando)
python run.py --experimentos        # además D.1 y D.2
python preguntas.py --todas         # solo P1, P2, P3, P4 y P6
python p5_salting.py                # solo P5
python experimento_d1.py            # D.1; con --ack-antes, el contraste (XACK antes del put)
python experimento_d2.py aof        # D.2 con la configuración elegida; `ninguna` es la contraria
python experimento_d2.py aof --abrupto     # SIGKILL en vez de `docker compose restart`
python experimento_d2.py fsync      # costo de appendfsync
python experimento_d2.py perdida    # entradas perdidas con un SIGKILL justo después de escribir
```

## 6. Qué no funcionó

- **El tablero reanudado en D.2 terminó sin aplicar eventos.** La señal de "fin" (`t2:productor:fin`) que dejó la primera corrida sobrevivió al reinicio de Redis (gracias al AOF) y el consumidor nuevo vio "fin + sin atraso + sin pendientes" y terminó antes de que el productor reanudado la borrara: el productor la borraba solo después de generar los 7.200 eventos (~5 s). **Corrección:** el productor la borra apenas se conecta, y el experimento espera a que esté borrada antes de lanzar los consumidores. Lección: una señal de término guardada en Redis también es estado y también persiste.
- **La ventana de pérdida de D.2 se calculó mal al principio:** se restaron filas (391) en vez de eventos (400) y salió ~2.609; lo correcto es el atraso (`lag`) del grupo de ingesta, 2.600.
- **El total de escrituras del JMX (7.048) no coincidía con los 7.200 put.** `happybase` junta en una sola mutación los put a una misma fila dentro de un lote (lote de 500): 7.200 put − repetidos del mismo lote = 7.048.
- **Los 197 del contraste de D.1** (XACK antes del put) no eran 200: 3 de las 200 entradas del lote compartían clave con otra del mismo lote.
- **Qué haríamos distinto:** más buckets de salt que estaciones (con 16 estaciones el salt repartió 18 %–31 %); guardar la llegada también en el histórico; una alarma de atraso del consumidor antes de que `MAXLEN` recorte entradas sin leer; y repetir la muerte brusca justo después de escribir con varias repeticiones (`experimento_d2.py perdida`).
