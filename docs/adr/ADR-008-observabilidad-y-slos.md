# ADR-008: Observabilidad y objetivos de nivel de servicio (SLO)

| Campo | Valor |
|---|---|
| Estado | Aceptado (revisado el 2026-10-25 tras la revisión final: acciones para SLO-2 y SLO-4) |
| Fecha | 2026-10-09 |
| Decisores | Equipo de arquitectura del proyecto |
| Relacionados | ADR-003, ADR-004, ADR-006, ADR-009 |

## Contexto

El enunciado pide definir **umbrales concretos**, por ejemplo una alerta generada en menos de 2
segundos desde el evento, y **qué ocurre cuando se incumplen, más allá de tener dashboards en
Grafana**. Un tablero en rojo que nadie mira no protege a nadie: el sistema debe medir, decidir y
actuar.

Hay dos tipos de información que observar:

- **Negocio:** alertas, eventos, falsos positivos, latencia de las alertas. Vive en TimescaleDB.
- **Técnica:** throughput, lag de los consumidores de Kafka, memoria, tiempos por etapa. Vive en los
  procesos.

## Opciones consideradas

| Criterio | Solo Grafana sobre TimescaleDB | Prometheus + Grafana + acción automática | Stack completo (ELK + Jaeger + Prometheus) |
|---|---|---|---|
| Métricas de negocio | Sí | Sí (TimescaleDB) | Sí |
| Métricas técnicas (lag, JVM, histogramas) | No | Sí (Micrometer → Prometheus) | Sí |
| Trazas distribuidas | No | No: se usa el `event_id` como identificador de trazabilidad | Sí |
| Acción ante un incumplimiento | Visual | **Visual + automática** (degradación y alerta de sistema) | Visual + alertas |
| Recursos | Bajos | Bajos (Prometheus ≈ 100 MB) | Altos (≈ 4 GB o más) |

## Decisión

**Prometheus** (métricas técnicas expuestas por Micrometer en `/actuator/prometheus`) y **Grafana**
con dos fuentes: TimescaleDB para el negocio y Prometheus para lo técnico. A eso se suma un **monitor
de SLO en Java** que actúa ante los incumplimientos.

### SLO definidos

| SLO | Indicador (SLI) | Objetivo | Ventana |
|---|---|---|---|
| **SLO-1 Latencia de alerta** | Tiempo entre el evento que completa la correlación y la alerta **guardada y visible** en la consola, para avisos HIGH/CRITICAL (primer aviso y agravamientos) | **95 % por debajo de 2 s** | 5 min móviles |
| **SLO-2 Pérdida de eventos críticos** | Eventos críticos (pánicos, reportes, disparos y explosiones con probabilidad ≥ 0,5; ADR-006) que no se pudieron enviar ni guardar en el spool | **0** | Siempre |
| **SLO-3 Frescura de datos** | p95 del tiempo entre el evento y su fila guardada en TimescaleDB | **< 10 s** | 1 min |
| **SLO-4 Rezago de procesamiento** | Atraso total de los grupos `ingestion`, `correlation`, `event-store` y `alert-store` en Kafka | < 1 000 mensajes (incumple si es ≥ 1 000 y no baja en dos mediciones seguidas) | 2 mediciones de 30 s |

### Qué ocurre cuando se incumplen

Cada incumplimiento produce una **alerta de sistema `SLO_INCUMPLIDO`** que se guarda y llega **en vivo**
a las consolas abiertas (por el mismo canal SSE que las demás alertas), además del registro en
`slo_measurements` y del panel correspondiente en "SLOs y Salud".

| SLO incumplido | Qué se hace |
|---|---|
| **SLO-1 o SLO-3** | Se determina la causa (tabla siguiente) y se actúa según ella |
| **SLO-4 solo** (el atraso es de 1 000 mensajes o más y no baja, aunque la latencia y la frescura se vean bien) | Causa **REZAGO**: un consumidor está trabado (p. ej. Redis caído detiene la correlación) y, como no deja filas ni alertas nuevas, SLO-1 y SLO-3 no lo verían. Alerta MEDIUM "Rezago de procesamiento sostenido" con el procedimiento, una vez cada 10 min. No se degrada: no es exceso de eventos |
| **SLO-2** | El simulador que no pudo guardar un evento crítico lo informa al alert-service en cuanto este responde (en una caída larga, al volver): alerta **CRITICAL** "Eventos críticos perdidos" con el nodo y la cantidad, registrada en la auditoría. El panel SLO-2 se pone en rojo de inmediato (Prometheus) |

El `SloMonitorJob` corre cada 30 s (ADR-004). Si SLO-1 o SLO-3 se incumplen, primero determina la
**causa**, porque no toda alerta tardía es una sobrecarga:

| Causa | Cómo se reconoce | Qué se hace |
|---|---|---|
| **SOBRECARGA** | El atraso de los cuatro grupos de consumidores de Kafka es grande (≥ 1 000 mensajes) y no baja | Los pasos 1 a 5 de abajo |
| **RECUPERACIÓN** | El atraso es bajo o va bajando: las alertas tardías son eventos que esperaron durante una caída ya superada (ADR-006) | Se registra con su causa y se avisa una vez al operador (alerta MEDIUM "Puesta al día tras una caída"). **No** se degrada: descartaría datos justo mientras el sistema se pone al día |

Si el atraso no se puede medir, se usa el último minuto: si también incumple, se trata como
sobrecarga. Ante una SOBRECARGA:

1. **Degradación controlada automática:** activa la bandera `system:degraded` en Redis. La ingesta lo
   detecta en menos de 5 s y empieza a **conservar solo el 10 % de las lecturas de velocidad
   normales**, el mayor volumen del sistema, que no participa en la correlación. Así se libera
   capacidad para pánicos, disparos y lecturas anómalas, que nunca se descartan.
2. **Alerta de sistema `SLO_INCUMPLIDO`** en la consola del operador, con el detalle (p95,
   porcentaje de cumplimiento, frescura): el supervisor sabe que los tiempos no son confiables.
3. **Registro** en `slo_measurements`, con la causa y el atraso de Kafka, para el informe de
   cumplimiento (panel "Incumplimientos de SLO").
4. **Procedimiento (runbook) para el supervisor:**
   - revisar el panel de lag por servicio;
   - si el cuello de botella es la correlación o la ingesta, levantar *workers* en un nodo C/D
     (`WORKERS=true` en `.env` y `centinela.cmd iniciar`), y Kafka reparte la carga solo;
   - si es la base de datos, la correlación en tiempo real sigue funcionando y lo histórico se pone
     al día al recuperarse.
5. **Recuperación:** cuando la medición vuelve a cumplir, se borra la bandera y la ingesta vuelve a
   guardar todo.

### Resultados medidos

| Escenario | Volumen | p50 | p95 | Máx. | ¿Cumple SLO-1? |
|---|---|---|---|---|---|
| Operación normal (validación: 8 escenarios, 9 alertas) | 13,5 ev/s | 44–78 ms | < 500 ms | 477 ms | Sí |
| Pico de diseño ×20 (versión final, multi-nodo) | 270 ev/s | 39 ms | 67 ms | 80 ms | Sí |
| Sobrecarga ×100 (primera versión, 1 nodo) | 1 350 ev/s pedidos, ≈ 820 procesados | 37 s | 240 s | 275 s | **No**: se activó la degradación y la alerta de sistema |
| Sobrecarga ×100 (tras optimizar el motor) | 1 350 ev/s pedidos, ≈ 850 procesados | 70 ms | 5,0 s | 8,4 s | **No**, pero con una mejora de 50 veces; el cuello de botella pasó a la ingesta |
| Caída del nodo B durante 93 s (puesta al día) | 13,5 ev/s | — | 90,7 s | 90,7 s | **No**, causa **RECUPERACIÓN** (atraso 0): aviso MEDIUM, sin degradar. Antes de la corrección se clasificaba como sobrecarga y activaba la degradación por error |

La prueba de sobrecarga se usó como **ejercicio de capacidad**. Las métricas de lag mostraron que el
motor de correlación era el cuello de botella. Se aplicaron dos optimizaciones: filtrar temprano los
eventos que no pueden coincidir con ninguna regla y enviar los comandos de cada ventana a Redis en un
solo viaje (*pipeline*). Con ellas el lag del motor quedó en 0 y la mediana bajó de 37 s a 70 ms (más
de 500 veces; el p95, de 240 s a 5 s, unas 50 veces). Con 5 veces el pico de diseño se pidió 1 350
ev/s y el equipo de prueba procesó ≈ 850, unas 3 veces el pico: ahí ya se incumple el SLO-1. Por
encima del pico de diseño, la respuesta es escalar horizontalmente (ADR-005).

## Consecuencias

**Positivas**
- Los SLO son medibles, están registrados y **se aplican solos**: el sistema protege lo crítico sin
  intervención humana.
- La trazabilidad funciona de punta a punta con el `event_id`: cada alerta lista los eventos de su
  evidencia, con su dato no estructurado (p. ej. el texto del reporte ciudadano), y la consola abre el
  evento guardado con un clic (`GET /api/events/{id}`).
- Tres dashboards provisionados como código (centro de monitoreo, SLO y salud, falsos positivos).
- Las tarjetas de latencia de Grafana y de la consola miden en la ventana del SLO-1 (5 min), no en el
  rango del tablero: tras una caída ya superada vuelven a la normalidad en unos 5 minutos, y el pico
  queda en la serie de tiempo y en la tabla de incumplimientos, con su causa.

**Negativas (aceptadas)**
- No hay trazas distribuidas de tipo Jaeger: se considera suficiente el `event_id` más las métricas por etapa.
- La medición de latencia depende de que los relojes de los nodos estén sincronizados; se exige el
  servicio de hora de Windows (NTP) en todas las PCs, y `centinela.cmd estado` avisa si hay desfase.
  Un desfase solo distorsiona la medición: la detección de la puesta al día ya no usa relojes (ADR-006).
- La degradación sacrifica detalle estadístico de tránsito (velocidades normales) mientras dura.

## Impacto en el negocio

- **Costo:** US$0 en licencias; Prometheus y Grafana corren en el servidor B ya presupuestado
  (ADR-001), sin hardware adicional.
- **Riesgo:** la dirección recibe un compromiso medible ("95 % de las alertas en menos de 2 s") y
  evidencia de cumplimiento. Cuando hay sobrecarga, el sistema prioriza solo las emergencias en lugar
  de colapsar de forma pareja.
- **Tiempo:** incluido y funcionando. El supervisor solo necesita conocer el procedimiento de
  escalado, que se resume en un comando.
