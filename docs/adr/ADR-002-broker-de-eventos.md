# ADR-002: Broker de eventos Apache Kafka (modo KRaft)

| Campo | Valor |
|---|---|
| Estado | Aceptado |
| Fecha | 2026-10-09 |
| Decisores | Equipo de arquitectura del proyecto |
| Relacionados | ADR-001, ADR-005, ADR-006, ADR-009 |

## Contexto

Cinco tipos de sensores generan eventos continuamente: unos 13,5 por segundo en operación normal y
hasta 270 por segundo en picos. Esos eventos los consumen, **cada uno a su propio ritmo**, tres
procesos distintos:

1. La ingesta, que valida y normaliza.
2. El motor de correlación, en tiempo real.
3. El almacenamiento histórico en TimescaleDB.

Hace falta un intermediario (broker) que desacople a quien produce de quien consume, absorba los
picos, no pierda eventos si un consumidor se cae, conserve el orden de los eventos de una misma zona
(clave para correlacionar) y permita repartir el procesamiento entre varias máquinas. Por ADR-001,
el broker debe correr **on-premise**.

## Opciones consideradas

| Criterio | Apache Kafka (KRaft) | RabbitMQ | Servicio gestionado (SQS / Pub/Sub) |
|---|---|---|---|
| Modelo | Log distribuido y persistente: los mensajes quedan disponibles durante la retención | Colas: el mensaje se borra al consumirse | Colas o tópicos gestionados en la nube |
| Varios consumidores leyendo lo mismo | Nativo: cada grupo lleva su propio avance (offset) | Requiere un exchange *fanout* y una cola por consumidor | Pub/Sub sí; SQS requiere SNS + varias colas |
| Reprocesar historia | Sí (reiniciar el offset) | No: el mensaje ya se borró | Limitado |
| Orden | Garantizado por partición (llave = zona) | Por cola; se pierde con varios consumidores | SQS FIFO con límites de throughput |
| Throughput | Muy alto (cientos de miles de msg/s por broker) | Alto (decenas de miles de msg/s) | Alto, pero se paga por mensaje |
| Escalado horizontal de consumidores | Automático (reparto de particiones) | Consumidores compitiendo; sin orden | Automático |
| Operación on-premise | Media: desde la versión 3.3 KRaft elimina ZooKeeper | Fácil | No aplica: contradice ADR-001 |
| Costo | Libre (Apache 2.0) | Libre (MPL 2.0) | Variable por mensaje y requiere internet |

## Decisión

Se adopta **Apache Kafka 3.9 en modo KRaft** (sin ZooKeeper), con este diseño de topics:

| Topic | Particiones | Retención | Llave | Uso |
|---|---|---|---|---|
| `sensor.panic.raw`, `sensor.lpr.raw`, `sensor.speed.raw`, `sensor.acoustic.raw`, `sensor.citizen.raw` | 12 | 24 h | id del sensor | Eventos crudos, **un topic por tipo de sensor** |
| `events.normalized` | 12 | 72 h | **id de zona** | Eventos validados y anonimizados |
| `alerts` | 3 | 7 días | id de zona | Alertas del motor |
| `events.dlq` | 1 | 7 días | — | Cola de mensajes muertos (ADR-006) |

Razones clave de diseño:

- **Un topic por tipo de sensor:** una avalancha de lecturas LPR no retrasa los botones de pánico,
  porque cada topic tiene sus propios hilos consumidores. Además, cada esquema evoluciona por separado.
- **Llave = zona en `events.normalized`:** todos los eventos de una zona llegan **en orden** a la
  misma instancia del motor de correlación.
- **12 particiones:** permiten hasta 12 consumidores en paralelo por grupo, por ejemplo 2 máquinas
  con 6 hilos cada una. Se probó con el motor repartido entre los nodos B y C: Kafka asignó 6
  particiones a cada uno y, al apagar el nodo C, el B tomó las 12 en segundos.
- **Productor con `acks=all` e idempotencia:** el broker confirma solo lo que quedó escrito y los
  reintentos no duplican mensajes.

## Consecuencias

**Positivas**
- Tres grupos de consumidores (ingestion, correlation, event-store) leen los mismos eventos sin
  estorbarse. Si la base de datos se pone lenta, la correlación en tiempo real no se entera.
- Si un consumidor cae, los eventos esperan en Kafka (72 h) y se procesan al volver, sin pérdida.
- Se puede escalar agregando instancias, sin cambiar código ni configuración.
- Los mensajes son JSON legible: se pueden inyectar a mano con `kafka-console-producer` durante la
  demo (por ejemplo, para mostrar la DLQ).

**Negativas (aceptadas)**
- Kafka es más complejo de operar que RabbitMQ: particiones, grupos y retención.
- En la simulación hay un solo broker (factor de replicación 1). Si el nodo A cae, no se pueden
  publicar eventos: los críticos quedan en un buffer en disco (ADR-006). En producción se recomiendan
  3 brokers con replicación 3.
- Consume más memoria que RabbitMQ (≈ 1 GB de heap).

## Impacto en el negocio

- **Costo:** cero en licencias; corre en el servidor del nodo A ya presupuestado (ADR-001).
- **Riesgo:** reduce el riesgo de perder eventos ante caídas parciales. Kafka es el estándar de la
  industria para este tipo de problema, así que es fácil conseguir personal y soporte.
- **Tiempo:** la configuración ya está automatizada en Docker Compose. Crecer (más sensores, más
  zonas) no exige rediseñar nada: basta con agregar máquinas.
