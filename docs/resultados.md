# Evaluación de resultados de la simulación

Las mediciones se tomaron el 9, 10, 15 y 25 de octubre de 2026 en un solo equipo (4 núcleos, 16 GB de RAM).
Primero se usó el modo de una sola máquina; después, el modo **multi-nodo**: los composes de los
nodos A, B y C (con *workers*), levantados como proyectos independientes y conectados por IP, igual
que en el laboratorio. Todos los servicios compartían los mismos 4 núcleos, así que las cifras son un
**límite inferior** de lo que daría el hardware dedicado propuesto en el ADR-001.

## 1. Validación de correlaciones esperadas

Se ejecuta con `POST /api/validation` en el simulador. Lanza en paralelo los 8 escenarios, cada uno
en su zona, y busca en la API de alertas las 9 alertas esperadas con su severidad mínima y su tiempo
límite. Corrida final, multi-nodo, con despliegue limpio:

| Escenario | Alerta esperada | Zona | Severidad | Detectada a los | Latencia evento → alerta | Resultado |
|---|---|---|---|---|---|---|
| Robo en curso (tabla 2.4, fila 1) | ROBO_EN_CURSO | 4 | CRITICAL | 18,0 s (al llegar la lectura LPR) | 44 ms | ✔ |
| Robo en curso: fuga | VEHICULO_EN_FUGA | 8 | CRITICAL | 90,0 s (cuando la placa aparece) | 28 ms | ✔ |
| Falso positivo (tabla 2.4, fila 2) | POSIBLE_FALSO_POSITIVO | 10 | LOW | 91,0 s (plazo de 90 s) | n/a* | ✔ |
| Accidente vial | ACCIDENTE_VIAL | 7 | CRITICAL | 20,0 s | 78 ms | ✔ |
| Disparos | DISPAROS_CONFIRMADOS | 2 | CRITICAL | 10,1 s | 65 ms | ✔ |
| Incendio | INCENDIO_ESTRUCTURAL | 11 | CRITICAL | 15,1 s | 74 ms | ✔ |
| Vehículo robado | VEHICULO_REPORTADO | 12 | HIGH | 0,3 s | 440 ms | ✔ |
| Botón abusivo | DISPOSITIVO_SOSPECHOSO | 1 | MEDIUM | 25,0 s (6.ª pulsación) | 42 ms | ✔ |
| Emergencia médica | EMERGENCIA_MEDICA | 5 | HIGH | 0,3 s | 477 ms | ✔ |

\* El falso positivo se clasifica por diseño al vencer el plazo de corroboración. Su "latencia" es ese
plazo y no cuenta para el SLO.

**Resultado: 9 de 9.** La validación se repitió 6 veces, en una sola máquina y en multi-nodo. Tras
agregar el reinicio del estado de correlación (ver hallazgos), todas las corridas dieron 9 de 9,
incluidas dos consecutivas. En la revisión final (25 de octubre) se repitió en multi-nodo con el
tráfico de fondo encendido y los escenarios aislados del ruido: 9 de 9 en todas las corridas (sección
5.3), y el escenario `tabla-2-4` lanzado justo después de una validación dio un ROBO_EN_CURSO nuevo en
la Zona 4 y el posible falso positivo en la Zona 10.

### 1.1 Falsos positivos con el tráfico de fondo (25 de octubre)

El tráfico de fondo no contiene incidentes reales: sus pánicos, reportes, sonidos y excesos de
velocidad se generan al azar. Se dejó el sistema una hora en operación normal (17:20 a 18:20 UTC,
multi-nodo, 48 513 eventos, sin escenarios) y se clasificó cada ancla según lo que hizo el motor:

| Regla | Anclas | Posible falso positivo | Alerta por coincidencia | Sumada a un incidente abierto |
|---|---|---|---|---|
| R1 Robo en curso | 31 | 24 (77 %) | 6 (19 %), todas HIGH con 0,70 | 1 |
| R2 Accidente vial (reporte) | 17 | 9 (53 %) | 7 (41 %): 4 con 0,70, una con 0,75, una con 0,85 y una CRITICAL con 1,00 | 1 |
| R5 Incendio | 7 | 7 (100 %) | 0 | 0 |
| **Total con verificación** | **55** | **40 (73 %)** | **13 (24 %)** | **2** |
| R7 Emergencia médica | 7 | no aplica | 7 (no requiere corroboración) | 0 |

En esa hora no hubo anclas de R3 ni de R4 (choque o disparo con probabilidad ≥ 0,7).

- Diez de las trece coincidencias sumaron exactamente 0,70 y nueve de ellas con una sola evidencia:
  un exceso de velocidad o una contravía al azar en la zona. El fondo produjo 36 anomalías de los
  sensores de velocidad (3 por zona y hora) y 27 excesos en cámaras LPR (2,3 por zona y hora); una
  ventana de 7 minutos contiene una anomalía de velocidad con probabilidad ≈ 30 %.
- R2 es la más expuesta: su ancla pesa 0,45 y cualquier exceso de velocidad (0,25), de sensor o de
  cámara, completa el umbral.
- La mitigación prevista es el ajuste adaptativo: con al menos 5 validaciones y precisión < 50 % en
  una regla y zona, el umbral sube a 0,80 y hacen falta dos evidencias: 11 de las 13 coincidencias
  no habrían alertado. Esta misma coincidencia
  causaba el defecto 14 (sección 5.3), resuelto aislando los escenarios del ruido.

Método: cada ancla de `sensor_events` se buscó por su `event_id` en la evidencia de las alertas de la
ventana (tabla `alerts`), para saber si terminó como posible falso positivo, como alerta de su regla o
dentro de un incidente ya abierto.

## 2. Comportamiento ante picos de carga

La operación normal simulada es de 13,5 eventos/s en las 12 zonas.

| Prueba | Configuración | Eventos/s procesados | p50 | p95 | Máx. | Frescura p95 | SLO-1 |
|---|---|---|---|---|---|---|---|
| Normal | 1 nodo | 13,5 | 44–78 ms | < 500 ms | 477 ms | < 50 ms | ✔ |
| Pico ×20 (primera versión) | 1 nodo | 270 | 253 ms | 608 ms | 831 ms | 86 ms | ✔ |
| **Pico ×20 (versión final)** | **multi-nodo** | **270** | **39 ms** | **67 ms** | **80 ms** | **47 ms** | **✔** |
| Sobrecarga ×100 (primera versión) | 1 nodo | ≈ 820 de 1 350 | 37 s | 240 s | 275 s | — | ✘, degradación activada |
| Sobrecarga ×100 (motor optimizado) | 1 nodo | ≈ 850 de 1 350 | 70 ms | 5,0 s | 8,4 s | — | ✘, degradación activada |

**Análisis:**

- En el **pico de diseño** (×20, por ejemplo un concierto o un feriado), la versión final cumple el
  objetivo de 2 s con un margen de unas 30 veces (p95 = 67 ms).
- La **sobrecarga ×100** se usó para encontrar el punto de quiebre:
  - Las métricas de lag (Prometheus) mostraron que el **motor de correlación** era el cuello de
    botella: 5 762 mensajes pendientes.
  - Se aplicaron dos optimizaciones: **filtrado temprano** de los eventos que no cumplen ninguna
    regla y **pipelining** de los comandos de Redis. Con ellas, el lag del motor quedó en 0 y la
    mediana bajó de 37 s a 70 ms.
  - El nuevo cuello de botella es la ingesta: ≈ 850 eventos/s en 4 núcleos compartidos con Kafka,
    TimescaleDB y Grafana.
- **En los dos casos de sobrecarga, el sistema actuó solo.** Activó la **degradación controlada**: en
  la segunda prueba muestreó 38 743 lecturas de velocidad normales. También publicó la alerta de
  sistema `SLO_INCUMPLIDO` en la consola del operador.
- Por encima del pico de diseño la respuesta prevista es el **escalado horizontal**: levantar
  *workers* en los nodos C/D (ver la sección 4). Con 5 veces el pico, el equipo de prueba procesó
  ≈ 850 eventos/s (unas 3 veces el pico) y ya incumplía el SLO-1.

## 3. Pruebas de resiliencia

### 3.1 Caídas de los servidores (15 de octubre, versión final)

Se apagó de verdad cada servidor con `centinela.cmd caida A|B`, con tráfico de fondo, un escenario
en curso y la consola del operador abierta en un navegador automatizado (Playwright) que registraba
lo que veía el operador.

| Prueba | Qué se hizo | Resultado medido |
|---|---|---|
| **Caída del nodo B** (93 s), workers en C | Se apagó B y se lanzó `disparos` en la Zona 2 | El vigilante declaró **NODO B SIN RESPUESTA a los 16 s** y dijo "La detección continúa en: correlation-engine-nodo-c". El worker de C generó **DISPAROS_CONFIRMADOS CRITICAL 10 s después** del escenario. Mientras tanto, 433 eventos esperaban en Kafka al grupo `event-store` (sin consumidor). La consola mostró "Sin conexión…" y, al volver B, "Conexión restablecida tras 107 s: 1 alerta…", destacada como "LLEGÓ 91 s TARDE". SLO: **RECUPERACIÓN**, sin modo degradado. |
| **Caída del nodo A** (82 s): Kafka, base y Redis | Se apagó A, se lanzó `robo-en-curso` en la Zona 4 y se envió un pánico por la app (`POST /api/v1/panic`) | El gateway respondió **202 `EN_COLA`**. El simulador guardó en disco 4 críticos y 222 eventos de telemetría útil. Al volver A, el spool se vació en 3 s, la lista de placas robadas se recargó sola a los 20 s y el robo salió como **ROBO_EN_CURSO HIGH → CRITICAL** (no como falso positivo). El pánico de la app quedó guardado y generó EMERGENCIA_MEDICA. Un `vehiculo-robado` lanzado después dio VEHICULO_REPORTADO en 38 ms. SLO: **RECUPERACIÓN** en las 5 mediciones, sin modo degradado. El vigilante dijo "Causa probable: el nodo A (datos) no responde". **0 eventos críticos perdidos.** |
| **Redis caído** (45 s) | Se detuvo solo Redis y se lanzó `disparos` | **Nada fue a la DLQ** (0 → 0). La ingesta siguió sin deduplicar con un solo aviso; el motor reintentó como falla transitoria. Al volver Redis salió DISPAROS_CONFIRMADOS y la lista de placas se recargó a los 33 s. Ninguna traza de error en los logs. |
| **`kill -9` del simulador con Kafka caído** | Se detuvo Kafka, se lanzó `emergencia-medica` y se mató el proceso del simulador | Al arrancar, el diario *write-ahead* devolvió "1 mensajes pendientes recuperados"; se reenvió y salió EMERGENCIA_MEDICA. **0 perdidos.** |
| **Mensajes inválidos** | 3 mensajes con `kafka-console-producer`: JSON roto, timestamp en el futuro y tipo de alerta inexistente | Los 3 en `events.dlq` como **PERMANENTE** (sin reintentos), visibles con `centinela.cmd dlq`. El reproceso por defecto los saltó (3 leídos, 0 reinyectados), como debe ser. |

### 3.2 Revisión final (25 de octubre)

| Prueba | Qué se hizo | Resultado medido |
|---|---|---|
| **Redis congelado 2,5 min** sin otro escenario | `docker pause` del contenedor de Redis con tráfico de fondo | La correlación se detuvo y su atraso subió a 741, 1 200 y 1 631 mensajes en tres mediciones; a la tercera el monitor registró el incumplimiento con causa **REZAGO** (SLO-4) y emitió la alerta MEDIUM "Rezago de procesamiento sostenido", que llegó en vivo a una consola conectada. Al volver Redis: "Puesta al día tras una caída" (MEDIUM), sin modo degradado. Esta prueba destapó un defecto propio, corregido: con la primera versión de la detección sin relojes, dos pánicos del fondo se clasificaron como falso positivo en el instante en que volvió Redis, con la correlación todavía atrasada. |
| **Redis congelado 2 min** en medio de un robo | `escenario robo-en-curso -Zona 3`; a los 5 s, `docker pause` de Redis (la velocidad y la placa llegaron durante la falla) | Al volver Redis, el consumidor reintentó, procesó la evidencia atrapada en Kafka y salió **ROBO_EN_CURSO CRITICAL** (latencia 134 s, por la falla). Ese pánico **no** se clasificó como falso positivo. 0 mensajes a la DLQ. |
| **Arranque simultáneo de todo** (26 de octubre) | Se reinició el equipo: los 12 contenedores de A, B y C arrancaron a la vez y sin orden, como tras un apagón | En unos 2 min todos estaban sanos y el vigilante declaró el nodo B OPERATIVO. Redis, que no guarda nada en disco, arrancó vacío y el alert-service recargó la lista de placas robadas al iniciar (6 placas). La validación dio **9 de 9**, con latencias de 20 a 313 ms. |
| **Reparto de particiones** | Tráfico de `events.normalized` por partición durante 40 s | Las 12 particiones con tráfico (36 a 57 eventos cada una) y cada uno de los 12 consumidores (6 en B, 6 en C) con una partición; antes, 5 particiones vacías. |

### 3.3 Primera ronda (9 y 10 de octubre)

| Prueba | Procedimiento | Resultado |
|---|---|---|
| Mensajes inválidos | 3 mensajes inyectados con `kafka-console-producer` | Los 3 fueron a `events.dlq` sin reintentos, con su causa y el topic de origen. El resto del flujo no se afectó. |
| Caída de Kafka | Se detuvo el broker y se presionó un botón de emergencia médica | El evento quedó en el spool en disco y se reenvió 7 s después de reiniciar Kafka (latencia de 14,5 s, que incluye la caída). |
| Caída de una instancia | Se apagó el worker de correlación del nodo C | Kafka reasignó sus particiones al nodo B. Una alerta de disparos se generó en 188 ms con el worker caído. Al volver, se repartieron 6 y 6. |
| Caídas de nodos completos (versión anterior) | Caída de B (con y sin workers), de A, de ambos y apagón total | Funcionaban la retención en Kafka y el spool de críticos, pero se encontraron las 8 brechas de la sección 5.2. |

## 4. Distribución de carga entre máquinas

Con el nodo B y el worker del nodo C activos, el grupo de consumidores `correlation` quedó así:

```
particiones de correlation-engine-nodo-b: 6
particiones de correlation-engine-nodo-c: 6
```

Con el hash por defecto de Kafka, las 12 llaves de zona caían en solo 7 particiones: 5 de los 12
hilos de consumo quedaban sin trabajo y otros procesaban tres zonas. Desde la revisión final, un
particionador propio envía la zona N a la partición N − 1 y las 12 particiones reciben tráfico.

La validación completa dio 9 de 9 en este modo. Las correlaciones funcionan aunque la evidencia de
un mismo incidente la procesen instancias distintas, como la fuga de la Zona 4 a la 8, porque el
estado vive en Redis compartido.

## 5. Hallazgos y correcciones durante las pruebas

### 5.1 Primera ronda

Probar escenarios de falla, y no solo el camino feliz, descubrió cuatro defectos reales:

| # | Hallazgo | Causa | Corrección |
|---|---|---|---|
| 1 | En la prueba de caída de Kafka los eventos críticos no se guardaban en el spool | El volumen Docker se creaba con permisos de root y la aplicación corre sin privilegios | El `Dockerfile` crea `/data/spool` con el usuario de la aplicación. Además, si el spool falla, el evento se cuenta como `lost` con un error explícito; antes el log decía "guardado" por error. |
| 2 | Al repetir el escenario de robo, no se detectaba la fuga del vehículo | El registro de "zonas donde ya se vio la placa" era por placa y no por incidente | El seguimiento es ahora por incidente; se agregó la prueba `samePlateInANewIncidentIsTrackedAgain`. |
| 3 | Un incidente abierto absorbía la evidencia nueva sin avisar | Solo se re-emitía la alerta si subía la severidad | Nueva revisión `UPDATED` cuando se suma un testigo o ancla; se agregó una prueba. La validación limpia primero el estado temporal para ser reproducible. |
| 4 | Se activó la degradación sin sobrecarga real | La corroboración tardía usaba como disparador el ancla de hace 90 s, lo que registraba una latencia falsa | El disparador es la evidencia más reciente. El SLO mide los avisos que el operador debe conocer de inmediato (NEW / ESCALATED). |

### 5.2 Caídas de nodos completos: 8 brechas y su corrección

Apagar cada servidor mostró diferencias entre lo que prometía el ADR-006 y lo que hacía el sistema.
Todas se corrigieron y se volvieron a probar (sección 3.1):

| # | Brecha | Cómo se supo | Corrección | Comprobación |
|---|---|---|---|---|
| 1 | Al volver A, la lista de placas robadas quedaba vacía hasta 10 min: un vehículo robado pasó sin alerta | Prueba | Llave centinela revisada cada 15 s y sincronización atómica | Recargada a los 20 s; VEHICULO_REPORTADO en 38 ms |
| 2 | Un robo durante la caída de A se clasificaba como POSIBLE_FALSO_POSITIVO | Prueba | Spool de telemetría útil (15 min) y pausa de la clasificación durante la puesta al día | ROBO_EN_CURSO CRITICAL |
| 3 | Nadie avisaba que B había caído | Prueba | Vigilante en el nodo C y aviso de conexión en la consola | Detectado a los 16 s; banner y alertas destacadas |
| 4 | Al recuperarse, el SLO activaba la degradación sin sobrecarga | Prueba | Causa por atraso de Kafka: sobrecarga solo si es grande y sostenido | RECUPERACIÓN, sin degradar |
| 5 | El spool se escribía cuando el envío ya había fallado: hasta 30 s en memoria | Código | Escritura anticipada con `fsync` y diario con CRC32C | `kill -9` sin pérdida |
| 6 | El gateway de la app respondía 500 si caía A | Código | Spool en el gateway: 202 `EN_COLA` | 202 y alerta al volver A |
| 7 | Lo que agotaba los reintentos iba a la DLQ y nadie lo reprocesaba | Código | Reintentos sin límite para fallas transitorias y reproceso automático de la DLQ | Redis caído 45 s: 0 en la DLQ |
| 8 | No había respaldo de la base | Configuración | `pg_dump` cada 6 h y `centinela.cmd restaurar` | Respaldo y restauración probados |

Al repetir las pruebas con las correcciones aparecieron tres detalles más, también corregidos: la
primera medición del SLO tras una caída (atraso de 1 030 mensajes mientras se vaciaba el spool) se
clasificaba como sobrecarga, por lo que ahora se exige un atraso **sostenido**; el vigilante
declaraba caído al alert-service cuando su chequeo de salud esperaba 30 s a la base, por lo que
ahora pregunta además si el proceso está vivo; y cada tarea periódica escribía la traza completa en
cada ronda durante la caída (cientos de líneas por minuto), por lo que ahora se resume en una línea.

Al grabar el video de la presentación, con el sistema en vivo, aparecieron tres defectos más. El video
los muestra fallando, explica la causa y los vuelve a probar corregidos:

| # | Defecto | Corrección | Verificación |
|---|---|---|---|
| 9 | Un segundo robo en una zona con el incidente abierto (2 a 5 min después del primero) salía como revisión recién a los 90 s: el motor solo probaba la coincidencia de mayor puntaje, la del primer pánico, que ya estaba en el incidente | Las coincidencias que alcanzan el umbral se prueban de mayor a menor puntaje hasta que una aporte algo nuevo | Prueba automatizada; en vivo, antes 74 s de atraso y después 36 ms |
| 10 | La regla R6 (vehículo reportado) sumaba el exceso de velocidad de una cámara que había leído **otra** placa, y escalaba a crítica la alerta del vehículo robado | Una cámara que identifica otra placa no corrobora; un sensor de velocidad, que no lee placas, sí | Prueba automatizada con el caso visto en vivo |
| 11 | El mapa de Grafana dependía de internet: sin los mosaicos de OpenStreetMap solo se veían puntos | Capa local con las 12 zonas (regiones del centroide más cercano, la misma regla de la ingesta) y su nombre | Tablero sin internet en el video |

### 5.3 Revisión final contra el enunciado (25 de octubre)

Una revisión completa de los requisitos del enunciado, del código y de la documentación encontró
estos defectos. Todos se corrigieron y se probaron en vivo en multi-nodo:

| # | Defecto | Corrección | Verificación |
|---|---|---|---|
| 12 | Las 12 zonas usaban solo 7 de las 12 particiones de `events.normalized` | Particionador propio: zona N → partición N − 1 | 12 de 12 particiones con tráfico; prueba automatizada |
| 13 | Con dos PCs cuyos relojes difieren 10 s o más, el motor se creía "atrasado" para siempre y no clasificaba ningún falso positivo | La puesta al día se detecta sin relojes: atraso que reporta Kafka, eventos reenviados de un spool y particiones que no logran procesar; espera de 40 s al arrancar, al rebalancear y al recuperarse | Pruebas automatizadas (reloj 60 s atrasado, partición que falla) y Redis congelado (sección 3.2) |
| 14 | Con el tráfico de fondo encendido, la fila 2 de la tabla 2.4 terminaba como robo en ≈ 15 % de las corridas (un exceso de velocidad al azar corroboraba el pánico) y repetir `tabla-2-4` mostraba el robo como revisión de otro incidente | Cada escenario aísla sus zonas del ruido de fondo (también en el otro generador) y arranca sin historial ni incidentes abiertos en las zonas de sus alertas | Validaciones 9/9 con el fondo encendido; `tabla-2-4` tras `validar` dio un robo nuevo |
| 15 | SLO-2 y SLO-4 no tenían acción; las alertas de sistema no llegaban en vivo a la consola | Alerta MEDIUM de rezago sostenido (SLO-4), alerta CRITICAL de eventos críticos perdidos (SLO-2) y envío en vivo por SSE; paneles de SLO-2 y SLO-4 en Grafana | Redis congelado: aviso a los 2 min, recibido en vivo |
| 16 | El texto de los reportes ciudadanos (dato no estructurado) se guardaba pero no se veía, y la trazabilidad exigía abrir la base | La evidencia trae un detalle (texto, vehículo, sentido) y la consola abre el evento guardado (`GET /api/events/{id}`); `centinela.cmd reporte` y `panico` envían eventos por la puerta HTTP de la app | Reporte con placa: texto visible y placa `P51***` |
| 17 | Las reglas "ajustables sin recompilar" estaban dentro de la imagen | Los contenedores del motor leen `infra/correlation/application.yml` | Arranque con la carpeta montada |
| 18 | Privacidad: el id de los celulares (un trozo de su seudónimo) era visible en Grafana; una placa escrita por un ciudadano quedaba en claro | Vista `v_events` sin ese id (migración V5), descripción de alertas sin él y placas enmascaradas en el texto libre y en los registros del simulador | Pruebas automatizadas y consulta a la base |
| 19 | El replay aceptaba rutas del contenedor y devolvía partes de cualquier archivo legible | Solo CSV incluidos en el simulador, con nombre validado | Prueba automatizada |
| 20 | Consola: "Cancelar" validaba la alerta igual; la evidencia abierta se cerraba con cada alerta nueva; cada reconexión breve del canal en vivo mostraba "Sin conexión" | Cancelar no valida; la evidencia abierta se conserva; aviso solo si el corte dura más de 6 s | Revisión en el navegador |
| 21 | Script de Windows (auditoría en PowerShell 5.1): un modo anterior encendido pasaba como "ya levantado", un clon nuevo fallaba con `BUILD_TARGET=prebuilt`, el firewall solo admitía la subred local, la IP se elegía mal con dos adaptadores, `fondo` y `pico` actuaban en un solo generador, entre otros | 11 correcciones, más el aviso de proxy, de contenedores Windows y de fallas al exportar o importar imágenes | Análisis sintáctico sin errores ni construcciones de PowerShell 7; `iniciar`, `escenario`, `validar`, `reporte` y `panico` ejecutados en este equipo |
| 22 | Tras una caída ya superada, la p95 de latencia en Grafana (rango de 30 min) y en la consola (última hora) seguía en decenas de segundos durante 30 a 60 min, aunque las alertas nuevas salían en milisegundos: las pocas alertas que esperaron durante la caída dominaban el percentil | Las tarjetas de latencia usan la ventana del SLO-1 (5 min, ADR-008); el pico sigue visible en la serie de tiempo y en la tabla de incumplimientos, con su causa | Caídas de B y de A con escenarios: alertas retenidas de 70 a 92 s y nuevas de 19 a 22 ms; a los 5 min de recuperar, p95 de 5 min = 36 ms mientras la de 30 min seguía en 88 s |

## 6. Pruebas automatizadas

`mvn install` (o `centinela.cmd pruebas`) ejecuta **119 pruebas unitarias**:

| Módulo | Pruebas | Qué cubren |
|---|---|---|
| common | 42 | Zonas, seudonimización HMAC y enmascarado de placas en texto libre; detalle de la evidencia; particionador por zona; diario *write-ahead* (corte de luz con línea truncada, compactación, reinicio con pendientes, topes, `fsync` agrupado concurrente); clasificación de fallas; manejadores de reintento; resumen de errores de tareas periódicas |
| sensor-simulator | 25 | Ruta de fuga, puntos dentro de su zona, CSV de replay (y rechazo de rutas), qué se guarda en disco si cae Kafka, publicación *write-ahead*, aislamiento de escenarios del ruido de fondo, aviso de eventos críticos perdidos (SLO-2), lógica del vigilante (incluye un servidor HTTP real que se cuelga) |
| ingestion-service | 16 | Seudonimización, generalización de ubicación, anomalías, rechazo de inválidos, deduplicación con ficha de entrega, gateway con spool |
| correlation-engine | 28 | El modelo completo con las reglas reales del YAML (17, incluye el segundo robo con el incidente abierto y la placa ajena en R6) y la resiliencia del motor (11: publicar antes de guardar, IDs deterministas, puesta al día detectada sin relojes, reloj desfasado entre PCs, partición que no logra procesar, espera al arrancar y al rebalancear) |
| alert-service | 8 | Reproceso de la DLQ y causa del incumplimiento del SLO |

La prueba del vigilante con un servidor HTTP que se cuelga falló una vez con la CPU saturada (los 12
contenedores arrancando a la vez): el plazo de 400 ms vencía también en la consulta de *liveness*. Se
amplió a 1,5 s sin cambiar lo que verifica, y pasa con la CPU al máximo. La corrida final dio 119 de
119.
