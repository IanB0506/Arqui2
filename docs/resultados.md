# Evaluación de resultados de la simulación

Las mediciones se tomaron el 9 y 10 de octubre de 2026 en un solo equipo (4 núcleos, 16 GB de RAM).
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
incluidas dos consecutivas.

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
- Más allá de unas 5 veces el pico de diseño, la respuesta prevista es el **escalado horizontal**:
  levantar *workers* en los nodos C/D (ver la sección 4).

## 3. Pruebas de resiliencia

| Prueba | Procedimiento | Resultado |
|---|---|---|
| Mensajes inválidos | 3 mensajes inyectados con `kafka-console-producer`: JSON roto, timestamp en el futuro y tipo de alerta inexistente | Los 3 fueron a `events.dlq` sin reintentos, con cabeceras que indican la causa y el topic de origen. El resto del flujo no se afectó. |
| Caída de Kafka | Se detuvo el broker y se presionó un botón de emergencia médica | El evento quedó en el spool en disco. Al reiniciar Kafka se reenvió en 7 s y la alerta llegó (latencia de 14,5 s, que incluye la caída). **0 eventos críticos perdidos.** La telemetría de esos segundos se descartó, como define la política. |
| Caída de una instancia | Se apagó el worker de correlación del nodo C | Kafka reasignó sus particiones al nodo B. Una alerta de disparos se generó en 188 ms con el worker caído. Al volver, se repartieron 6 y 6. |

## 4. Distribución de carga entre máquinas

Con el nodo B y el worker del nodo C activos, el grupo de consumidores `correlation` quedó así:

```
particiones de correlation-engine-nodo-b: 6
particiones de correlation-engine-nodo-c: 6
```

La validación completa dio 9 de 9 en este modo. Las correlaciones funcionan aunque la evidencia de
un mismo incidente la procesen instancias distintas, como la fuga de la Zona 4 a la 8, porque el
estado vive en Redis compartido.

## 5. Hallazgos y correcciones durante las pruebas

Probar escenarios de falla, y no solo el camino feliz, descubrió cuatro defectos reales:

| # | Hallazgo | Causa | Corrección |
|---|---|---|---|
| 1 | En la prueba de caída de Kafka los eventos críticos no se guardaban en el spool | El volumen Docker se creaba con permisos de root y la aplicación corre sin privilegios | El `Dockerfile` crea `/data/spool` con el usuario de la aplicación. Además, si el spool falla, el evento se cuenta como `lost` con un error explícito; antes el log decía "guardado" por error. |
| 2 | Al repetir el escenario de robo, no se detectaba la fuga del vehículo | El registro de "zonas donde ya se vio la placa" era por placa y no por incidente | El seguimiento es ahora por incidente; se agregó la prueba `samePlateInANewIncidentIsTrackedAgain`. |
| 3 | Un incidente abierto absorbía la evidencia nueva sin avisar | Solo se re-emitía la alerta si subía la severidad | Nueva revisión `UPDATED` cuando se suma un testigo o ancla; se agregó una prueba. La validación limpia primero el estado temporal para ser reproducible. |
| 4 | Se activó la degradación sin sobrecarga real | La corroboración tardía usaba como disparador el ancla de hace 90 s, lo que registraba una latencia falsa | El disparador es la evidencia más reciente. El SLO mide los avisos que el operador debe conocer de inmediato (NEW / ESCALATED). |

## 6. Pruebas automatizadas

`mvn install` ejecuta **26 pruebas unitarias**:

| Módulo | Pruebas | Qué cubren |
|---|---|---|
| common | 4 | Resolución de zonas, zona más cercana, vecinos, seudonimización HMAC |
| sensor-simulator | 2 | Ruta de fuga y lectura del CSV de replay |
| ingestion-service | 5 | Seudonimización de placas, generalización de ubicación, anomalías, rechazo de eventos inválidos |
| correlation-engine | 15 | El modelo completo, con las reglas reales del YAML |
