# ADR-006: Resiliencia y tolerancia a fallos

| Campo | Valor |
|---|---|
| Estado | Aceptado (revisado el 2026-10-15 tras las pruebas de caída de nodos) |
| Fecha | 2026-10-09 |
| Decisores | Equipo de arquitectura del proyecto |
| Relacionados | ADR-002, ADR-003, ADR-005, ADR-008, ADR-009 |

## Contexto

En un sistema de seguridad ciudadana, no todos los eventos valen lo mismo. Perder la pulsación de un
botón de pánico puede costar una vida; perder una de las miles de lecturas de velocidad normales de
un minuto no cambia nada. El enunciado pide definir:

- el manejo de caídas del broker;
- la cola de mensajes muertos (DLQ);
- los reintentos;
- la **pérdida aceptable de eventos**.

La arquitectura tiene dos servidores con papeles distintos: el **nodo A** guarda el estado (Kafka,
TimescaleDB, Redis) y el **nodo B** lo procesa (ingesta, correlación, alertas, Grafana). Los nodos
C/D generan los eventos y pueden correr *workers* de ingesta y correlación.

Fallas que hay que contemplar:

1. Kafka no disponible: cae el nodo A o la red.
2. Mensajes mal formados: un sensor defectuoso o un ataque.
3. Caídas temporales de Redis o de la base de datos.
4. Caída del nodo B o de una instancia de un microservicio.
5. Muerte de un proceso a mitad de una operación (alerta publicada pero estado sin guardar).
6. Duplicados generados por los reintentos.

La primera versión de este ADR se probó apagando de verdad cada nodo. Las pruebas encontraron ocho
brechas entre lo que el ADR prometía y lo que el sistema hacía; esta revisión las corrige y deja
constancia de ellas en la sección final.

## Opciones consideradas

| Aspecto | Opción A | Opción B | Elegida |
|---|---|---|---|
| Mensajes inválidos | Descartarlos y registrarlos en el log | Enviarlos a una **DLQ** con la causa | **B**: se pueden revisar y reprocesar |
| Reintentos | Un número fijo para todo error | **Según la clase de falla** (transitoria, permanente, desconocida) | **B**: una caída de Redis de 5 minutos no debe mandar alertas a la DLQ |
| Kafka caído, en el productor | Guardar en disco solo lo que ya falló | **Escritura anticipada (*write-ahead*)**: a disco antes de enviar | **B**: no hay ventana en que un crítico viva solo en memoria |
| Qué guardar si cae Kafka | Solo los críticos | **Críticos sin vencimiento (tope 32 MB) + telemetría útil con tope de 15 min** | **B**: sin la telemetría, un robo durante la caída se clasificaba como falso positivo |
| Garantía de entrega | Como máximo una vez (*at-most-once*) | **Al menos una vez + deduplicación + IDs deterministas** | **B** |
| Estado de la correlación | En la memoria de cada proceso | **En Redis compartido** | **B**: cualquier instancia puede reemplazar a otra |
| Detección de la caída de B | Que alguien note que la consola no carga | **Vigilante en el nodo C** + aviso en la consola | **B** |
| Alta disponibilidad del broker | 1 broker | 3 brokers con replicación 3 | **1 en la simulación**; 3 recomendado en producción |

## Decisión

### Política de pérdida aceptable

| Tipo de evento | Pérdida aceptable | Mecanismo |
|---|---|---|
| Botón de pánico, reporte ciudadano, disparo o explosión con probabilidad ≥ 0,5 (simulador) | **0** | Spool *write-ahead* con `fsync`, sin vencimiento; si el disco se llena (32 MB ≈ 100 000 eventos) se rechaza lo nuevo y se registra, nunca se descarta lo guardado |
| Pánico y reporte desde la app móvil (gateway `/api/v1/panic`) | **0** | Mismo spool en la ingesta: 202 `EN_COLA` si Kafka no responde; 503 con `Retry-After` solo si tampoco hay disco (la app reintenta con el mismo `event_id`) |
| Telemetría útil durante una caída de Kafka: todas las placas (LPR), excesos de velocidad, contravía y sonidos relevantes | Se conserva la de los **últimos 15 min** (tope 32 MB) | Spool de telemetría: vence a los 15 min porque más vieja ya no corrobora nada (ventanas de 15 min); si se llena, se descarta la más antigua |
| Lecturas de velocidad normales y ruido ambiental durante una caída de Kafka | Se pierden | No participan en ninguna regla; guardarlas llenaría el disco sin beneficio |
| Lecturas de velocidad **normales** en modo degradado (sobrecarga real) | Se conserva 1 de cada 10 | Muestreo en la ingesta (ADR-008) |
| Cualquier evento ya aceptado por Kafka | **0** (mientras la caída del consumidor dure menos que la retención) | Retención de 24 h en los topics crudos y 72 h en `events.normalized`, offset confirmado solo después de procesar y reintentos sin límite para fallas transitorias |
| Memoria de corto plazo (ventanas, incidentes abiertos) si se reinicia Redis | Se pierde | Aceptado: Redis corre sin persistencia por rendimiento. La lista de placas robadas, que sí importa, se reconstruye sola desde la base |

### Mecanismos implementados

**1. Productores confiables.** `acks=all` e `enable.idempotence=true`. El broker confirma solo lo
escrito y los reintentos internos no duplican. `max.block.ms` limita a 3–5 s la espera de `send()`
sin broker, para que un nodo A caído no congele al simulador ni al gateway.

**2. Spool *write-ahead* en disco** (`common/.../spool/DiskSpool`). Cada evento crítico se escribe
en un diario en disco **antes** de enviarlo y se marca como confirmado cuando Kafka responde:

- formato de líneas con suma de verificación CRC32C (`P` = pendiente, `A` = confirmado, `F` = falla),
  de modo que una línea a medio escribir por un corte de luz se detecta y se ignora;
- `fsync` agrupado (varios eventos por escritura física) y compactación atómica (archivo nuevo +
  `rename`) cuando el diario crece;
- al arrancar, se relee el diario y se reenvía lo pendiente: un `kill -9` del simulador con Kafka
  caído no pierde nada;
- al volver Kafka se drena primero lo crítico y luego la telemetría, a 500 eventos/s para no
  saturar al nodo B;
- los eventos reenviados llevan las cabeceras `centinela-first-attempt` y `centinela-spooled`. La
  ingesta los marca (`reenviado_spool`, `retraso_origen_s`) y mide su antigüedad contra la hora
  original, de modo que no se rechazan por "viejos" tras una caída larga.

| Spool | Qué guarda | Tope | Al llenarse |
|---|---|---|---|
| `critical.wal` (simulador) | Pánico, reporte ciudadano, disparo/explosión ≥ 0,5 | 32 MB, sin vencimiento | Rechaza lo nuevo y lo registra en el log; se ve como `rechazadosPorTope` en `/api/simulator/spool` y el evento perdido cuenta en `simulator_events_sent_total{result="lost"}` |
| `telemetry.wal` (simulador) | LPR, excesos, contravía, sonidos relevantes | 32 MB o 15 min | Descarta lo más antiguo |
| `gateway.wal` (ingesta) | Pánicos y reportes de la app | 32 MB, sin vencimiento | Responde 503 para que la app reintente |

**3. Reintentos según la clase de falla** (`FailureClassifier` + `ResilientErrorHandlers`, iguales
en los tres consumidores):

| Clase | Ejemplos | Reintentos | Destino final |
|---|---|---|---|
| **PERMANENTE** | JSON inválido, campos faltantes, zona inexistente, timestamp en el futuro | Ninguno | DLQ de inmediato |
| **TRANSITORIO** | Kafka, Redis o la base no responden (`SQLState 08*`/`57P*`, conexión rechazada, *timeout*) | **Sin límite**, con espera exponencial de 1 s a 30 s | Ninguno: el mensaje espera en Kafka hasta que vuelva la dependencia |
| **DESCONOCIDO** | Cualquier otro error | 4 | DLQ |

Mientras reintenta, el consumidor no avanza el offset, así que el mensaje no se pierde ni se salta.
Los reintentos se registran cada 10 intentos para no inundar el log.

**4. DLQ con reproceso.** Cada mensaje en `events.dlq` lleva el topic, la partición y el offset
originales, la causa y tres cabeceras propias: `centinela-error-class`, `centinela-service` y
`centinela-dlq-attempts`. El alert-service corre cada 5 min (con ShedLock, una sola instancia) una
tarea que **reinyecta en su topic original** lo que falló por causas transitorias o desconocidas,
hasta 3 veces por mensaje. Lo permanente solo se reprocesa a pedido de un administrador
(`centinela.cmd reprocesar-dlq -IncluirPermanentes`, auditado), por ejemplo tras corregir una regla de
validación. `centinela.cmd dlq` muestra el total por clase y los últimos mensajes.

**5. Deduplicación con ficha de entrega.** La ingesta reclama cada `event_id` en Redis con un script
Lua atómico que guarda también la "ficha" de la entrega (`topic:partición:offset`):

- `CLAIM`: si el `event_id` no existe, lo marca como *pendiente* (60 s) con la ficha; si existe con
  otra ficha, es un duplicado real y se descarta;
- `CONFIRM`: al publicar el evento normalizado, la marca pasa a *confirmada* (10 min);
- `RELEASE`: si el procesamiento falla, se borra la marca para que el reintento no se tome por
  duplicado.

Así, un reintento del **mismo** mensaje no se descarta (antes podía perderse) y un duplicado real
(otro envío del mismo evento) sí. Si el proceso muere con la marca pendiente, vence sola a los 60 s.
En la base de datos se mantiene `ON CONFLICT DO NOTHING` y en las alertas el *upsert* por `alert_id`
y revisión.

**6. Interruptor de Redis.** Si Redis cae, la ingesta **sigue publicando sin deduplicar** (se
prefiere disponibilidad: los duplicados se neutralizan en la base y en las alertas) y un único hilo
vuelve a probar Redis cada 5 s, en lugar de que cada evento espere el *timeout* de 2 s. El motor de
correlación, que sí necesita Redis para su estado, trata la caída como falla transitoria: reintenta
sin límite y nada va a la DLQ. Resultado: un solo aviso en el log y recuperación automática.

**7. Motor de correlación atómico.** El motor **publica la alerta antes de guardar su estado**
(marcar evidencia como usada, abrir el incidente, registrar al sospechoso) y cada alerta tiene un
**ID determinista** (UUID derivado de la regla, la zona y el evento ancla). Si el proceso muere entre
la publicación y el guardado, Kafka le entrega el evento a otra instancia, que recalcula la misma
alerta con el mismo ID, y la persistencia idempotente la deja una sola vez. Nunca queda estado
guardado de una alerta que no salió.

**8. Protección contra falsos positivos en la puesta al día.** Si el motor procesa eventos con más
de 10 s de atraso (Kafka acumuló mensajes durante una caída), lo anota en Redis
(`correlation:backlog`, 20 s) y **ninguna instancia clasifica falsos positivos** mientras tanto: la
evidencia que corrobora un pánico puede venir detrás en la cola. Si la publicación de un falso
positivo falla, el ancla vuelve a la lista de pendientes.

**9. Lista de placas robadas autorreparable.** La sincronización desde la base es atómica (se arma
en una llave temporal y se renombra) y deja una llave centinela `watchlist:synced`. El alert-service
revisa la centinela cada 15 s: si falta (Redis se reinició vacío), recarga la lista y los ajustes de
umbral de inmediato. El motor conserva su última copia buena mientras tanto, en lugar de quedarse
con una lista vacía.

**10. Servicios sin estado local y rebalanceo rápido.** Las ventanas, los incidentes y los
sospechosos viven en Redis. Si una instancia cae, Kafka reasigna sus particiones a otra en ~10 s
(`session.timeout.ms` = 10 s, latido cada 3 s; el valor por defecto era 45 s).

**11. Vigilante del nodo B** (en el simulador del nodo C). Cada 10 s consulta la salud de los
servicios de B (con 3 s de espera y 2 fallas seguidas para declarar caído, para no dar falsas
alarmas) y, con la API de administración de Kafka, qué grupos de consumidores tienen miembros y
cuánto atraso acumulan. Reporta **OPERATIVO / DEGRADADO / SIN_RESPUESTA**, distingue "la máquina no
responde" de "el servicio no responde" y dice **dónde sigue la detección** (p. ej. "La detección
continúa en: correlation-engine-nodo-c") y qué se está acumulando sin consumidor. Se ve en
`/vigilante.html`, en `centinela.cmd estado` y en el log.

**12. Consola consciente de la conexión.** Si se corta el canal en vivo (SSE), la consola muestra
"Sin conexión con el servidor desde las HH:MM… reintentando". Al reconectar, pide las alertas
generadas durante el corte, las **destaca** y marca cuánto tardaron ("LLEGÓ 91 s TARDE"), para que el
operador sepa que no las vio en su momento.

**13. SLO que distingue sobrecarga de recuperación** (ADR-008). Si el SLO se incumple, se mira el
atraso de los cuatro grupos de consumidores: si es grande y no baja, es **SOBRECARGA** (se activa la
degradación); si es bajo o va bajando, es **RECUPERACIÓN** de una caída ya superada: se avisa una vez
al operador (alerta MEDIUM "Puesta al día tras una caída") y **no** se degrada, porque descartaría
datos justo durante la puesta al día.

**14. Operación.** Reinicio automático de contenedores (`restart: unless-stopped`); apagado ordenado
(el simulador y la ingesta terminan la ronda de reenvío y cierran el diario antes de salir);
`/actuator/health/liveness` separado de la salud completa, para que Docker no reinicie un servicio
sano solo porque Redis está caído; respaldo de la base con `pg_dump` cada 6 h (se guardan 8; una
base vacía no se respalda y un reinicio no rota respaldos buenos) y restauración con
`centinela.cmd restaurar`, que restaura en una base temporal y luego la intercambia por la actual,
sin detener los servicios.

### Pruebas realizadas en la simulación

Todas en modo multi-nodo (A, B y C con *workers*, como proyectos Docker independientes conectados
por IP), con tráfico de fondo y la consola del operador abierta en un navegador automatizado.

| Prueba | Resultado |
|---|---|
| **Caída del nodo B** (90 s) con un escenario de disparos | El vigilante declaró **NODO B SIN RESPUESTA a los 16 s** e informó "La detección continúa en: correlation-engine-nodo-c". El worker de C generó DISPAROS_CONFIRMADOS **10 s después del escenario**, con B caído. La consola mostró "Sin conexión…"; al volver B: "Conexión restablecida tras 107 s: 1 alerta…", destacada como "LLEGÓ 91 s TARDE". El SLO registró **RECUPERACIÓN**, sin modo degradado. 0 eventos perdidos. |
| **Caída del nodo A** (82 s) con un robo en curso y un pánico desde la app | El gateway respondió **202 `EN_COLA`**; el simulador guardó 4 críticos y 222 eventos de telemetría útil. Al volver A el spool se vació en 3 s, la lista de placas se recargó sola a los 20 s y el robo salió como **ROBO_EN_CURSO HIGH → CRITICAL** (antes: falso positivo). El pánico de la app generó su alerta. El vigilante dijo "Causa probable: el nodo A (datos) no responde". SLO: RECUPERACIÓN. 0 críticos perdidos. |
| **Redis caído** 45 s con un escenario de disparos | 0 mensajes a la DLQ; un solo aviso por servicio y ninguna traza de error; la alerta salió al volver Redis. |
| **`kill -9` del simulador** con Kafka caído | Al arrancar, el diario devolvió el evento pendiente, se reenvió y salió su alerta. 0 perdidos. |
| 3 mensajes inválidos inyectados con `kafka-console-producer` | Los 3 llegaron a `events.dlq` como PERMANENTE, sin reintentos; el reproceso por defecto no los tocó. El resto del flujo siguió. |
| Worker de correlación del nodo C apagado | Kafka reasignó sus 6 particiones al nodo B; una alerta de disparos se generó en 188 ms con el worker caído. Al volver, 6 y 6. |
| Pruebas automatizadas del diario, la clasificación de fallas, la deduplicación, el gateway, el vigilante, el SLO y el motor | 90 pruebas, todas en verde (incluye: línea truncada por corte de luz, compactación, reinicio con pendientes, reintento del mismo mensaje, muerte entre publicar y guardar). |

## Consecuencias

**Positivas**
- Ningún evento crítico depende de la memoria de un proceso: está en disco antes de salir.
- Una caída de B solo produce retraso, y ahora se **ve**: el vigilante la anuncia y la consola avisa.
- Una caída de A ya no convierte un robo real en falso positivo, ni deja la lista de placas vacía.
- Una caída larga de Redis o de la base ya no manda alertas a la DLQ; lo que llega a la DLQ se
  reprocesa solo si la causa era pasajera.
- Un mensaje dañado no bloquea el sistema ("mensaje veneno") y queda disponible para análisis.

**Negativas (aceptadas)**
- Con un solo broker y una sola base, el nodo A sigue siendo el **punto único de falla**: mientras
  está caído no se detecta nada nuevo, solo se guarda para después.
- "Al menos una vez" implica posibles duplicados, que la deduplicación y los IDs deterministas
  neutralizan (con un pequeño costo en Redis).
- El spool vive en la máquina generadora: si esa máquina pierde su disco durante una caída de
  Kafka, esos eventos se pierden.
- La memoria de corto plazo de Redis se pierde si se reinicia: un incidente abierto antes de la
  caída de A se abre como nuevo si llega más evidencia.
- `fsync` por grupo de eventos críticos cuesta unos milisegundos por evento; es irrelevante para su
  volumen (decenas por minuto).

## Brechas encontradas por las pruebas y su corrección

| # | Brecha | Corrección (mecanismo) |
|---|---|---|
| 1 | Al volver A, la lista de placas robadas quedaba vacía hasta 10 min | Centinela revisada cada 15 s y sincronización atómica (9) |
| 2 | Un robo durante la caída de A se clasificaba como falso positivo | Spool de telemetría útil (2) y pausa de clasificación en la puesta al día (8) |
| 3 | Nadie avisaba que B había caído | Vigilante en C (11) y aviso en la consola (12) |
| 4 | Al recuperarse, el SLO activaba el modo degradado sin sobrecarga | Causa del incumplimiento por atraso de Kafka (13) |
| 5 | El spool se escribía cuando el envío ya había fallado: hasta 30 s en memoria | Escritura anticipada con `fsync` (2) |
| 6 | El gateway de la app respondía 500 si caía A | Spool en el gateway, 202 `EN_COLA` (2) |
| 7 | Lo que agotaba los reintentos iba a la DLQ y nadie lo reprocesaba | Reintentos por clase de falla (3) y reproceso de la DLQ (4) |
| 8 | No había respaldo de la base | `pg_dump` programado y restauración (14); en producción, réplica de la base y Kafka con factor de réplica 3 |

## Impacto en el negocio

- **Costo:** sin costo adicional en la simulación. La alta disponibilidad completa en producción
  (3 brokers, una réplica de la base y Redis con Sentinel) costaría unos US$4 000 más en hardware.
- **Riesgo:** queda explícito qué se puede perder y qué no, una definición que el área legal y la
  dirección pueden revisar y aprobar. La pulsación de un ciudadano en peligro no se pierde aunque
  falle el servidor central, y si cae el servidor de servicios el supervisor se entera en segundos.
- **Tiempo:** los mecanismos están incluidos y probados; la recuperación no requiere intervención
  manual.
