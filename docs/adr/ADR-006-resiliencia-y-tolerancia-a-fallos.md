# ADR-006: Resiliencia y tolerancia a fallos

| Campo | Valor |
|---|---|
| Estado | Aceptado |
| Fecha | 2026-10-09 |
| Decisores | Equipo de arquitectura del proyecto |
| Relacionados | ADR-002, ADR-003, ADR-005, ADR-008 |

## Contexto

En un sistema de seguridad ciudadana, no todos los eventos valen lo mismo. Perder la pulsación de un
botón de pánico puede costar una vida; perder una de las miles de lecturas de velocidad normales de
un minuto no cambia nada. El enunciado pide definir:

- el manejo de caídas del broker;
- la cola de mensajes muertos (DLQ);
- los reintentos;
- la **pérdida aceptable de eventos**.

Fallas que hay que contemplar:

1. Kafka no disponible: cae el nodo A o la red.
2. Mensajes mal formados: un sensor defectuoso o un ataque.
3. Caídas temporales de Redis o de la base de datos.
4. Caída de una instancia de un microservicio.
5. Duplicados generados por los reintentos.

## Opciones consideradas

| Aspecto | Opción A | Opción B | Elegida |
|---|---|---|---|
| Mensajes inválidos | Descartarlos y registrarlos en el log | Enviarlos a una **DLQ** con la causa | **B**: se pueden revisar y reprocesar |
| Errores transitorios | Fallar de inmediato | **Reintentos con espera exponencial** | **B** |
| Kafka caído, en el productor | Descartar todo | **Buffer en disco solo para eventos críticos** | **B** |
| Garantía de entrega | Como máximo una vez (*at-most-once*) | **Al menos una vez (*at-least-once*) + deduplicación** | **B** |
| Estado de la correlación | En la memoria de cada proceso | **En Redis compartido** | **B**: cualquier instancia puede reemplazar a otra |
| Alta disponibilidad del broker | 1 broker | 3 brokers con replicación 3 | **1 en la simulación**; 3 recomendado en producción |

## Decisión

### Política de pérdida aceptable

| Tipo de evento | Pérdida aceptable | Mecanismo |
|---|---|---|
| Botón de pánico, reporte ciudadano | **0** | Buffer en disco (*spool*) en el simulador/pasarela y reenvío automático |
| Telemetría (LPR, velocidad, acústicos) durante una caída de Kafka | Se acepta perder la de esos minutos | Se descarta tras 30 s sin confirmación; se cuenta como `dropped` |
| Lecturas de velocidad **normales** en modo degradado | Se conserva 1 de cada 10 | Muestreo en la ingesta (ADR-008) |
| Cualquier evento ya aceptado por Kafka | **0** | Retención de 72 h y confirmación de offset solo después de procesar |

### Mecanismos implementados

1. **Productores confiables:** `acks=all` e `enable.idempotence=true`. El broker confirma solo lo
   escrito y los reintentos internos no duplican.
2. **Buffer en disco (*store and forward*)** en el simulador: si un evento crítico no se puede
   enviar, se guarda en `/data/spool` (volumen persistente) y se reintenta cada 10 s. Si el disco
   también falla, se registra como `lost` y se emite un log de error: nunca se pierde en silencio.
3. **Ingesta "al menos una vez":** el offset del mensaje crudo se confirma solo cuando Kafka aceptó
   el evento normalizado.
4. **Reintentos con espera exponencial:**
   - ingesta: 1 s, 2 s, 4 s y 8 s;
   - motor de correlación: hasta 2 minutos, porque es preferible esperar a Redis que perder correlaciones;
   - almacenamiento: reintento indefinido cada 5 s, porque los eventos esperan en Kafka.
5. **DLQ `events.dlq`:** los errores permanentes (JSON inválido, campos faltantes, zona inexistente,
   timestamp en el futuro) van directo, sin reintentos. Cada mensaje lleva cabeceras con el topic
   original, la partición, el offset y la causa.
6. **Deduplicación:**
   - en la ingesta, por `event_id` en Redis (`SET NX` con 10 min de vigencia);
   - en la base de datos, `ON CONFLICT DO NOTHING`;
   - en las alertas, `upsert` por `alert_id` y número de revisión.
7. **Servicios sin estado local:** las ventanas, los incidentes y los sospechosos viven en Redis. Si
   una instancia cae, Kafka reasigna sus particiones a otra, que continúa con el mismo contexto.
8. **Reinicio automático** de contenedores (`restart: unless-stopped`).

### Pruebas realizadas en la simulación

| Prueba | Resultado |
|---|---|
| 3 mensajes inválidos inyectados con `kafka-console-producer` | Los 3 llegaron a `events.dlq` con su causa (JSON roto, timestamp en el futuro, tipo de alerta inexistente). El sistema siguió procesando el resto. |
| Kafka detenido + botón de emergencia médica | El evento quedó en el spool (1 pendiente). Al reiniciar Kafka se reenvió en 7 s y la alerta llegó (latencia de 14,5 s, que incluye la caída). **0 eventos críticos perdidos.** |
| Worker de correlación del nodo C apagado | Kafka reasignó sus 6 particiones al nodo B. Una alerta de disparos se generó en 188 ms con el worker caído. Al volver, se repartieron de nuevo 6 y 6. |

La primera prueba de caída de Kafka **descubrió un defecto real**: el volumen del spool se creaba con
permisos de root y el proceso, que corre sin privilegios, no podía escribir. Se corrigió en el
`Dockerfile`, que ahora crea `/data/spool` con el usuario de la aplicación, y la prueba se repitió
con éxito. Esto muestra el valor de probar los escenarios de falla y no solo el camino feliz.

## Consecuencias

**Positivas**
- Los eventos críticos sobreviven a caídas del broker.
- Un mensaje dañado no bloquea el sistema ("mensaje veneno") y queda disponible para análisis.
- El procesamiento se recupera solo cuando vuelve la dependencia caída.

**Negativas (aceptadas)**
- Con un solo broker, una caída del nodo A detiene la telemetría durante la falla.
- "Al menos una vez" implica posibles duplicados, que la deduplicación neutraliza (con un pequeño
  costo en Redis).
- El spool vive en la máquina generadora: si esa máquina pierde su disco durante una caída de
  Kafka, esos eventos se pierden.

## Impacto en el negocio

- **Costo:** sin costo adicional en la simulación. La alta disponibilidad completa en producción
  (3 brokers y una réplica de la base de datos) costaría unos US$4 000 más en hardware.
- **Riesgo:** queda explícito qué se puede perder y qué no, una definición que el área legal y la
  dirección pueden revisar y aprobar. La pulsación de un ciudadano en peligro no se pierde aunque
  falle el servidor central.
- **Tiempo:** los mecanismos están incluidos y probados; no requieren intervención manual.
