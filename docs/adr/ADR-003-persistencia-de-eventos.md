# ADR-003: Persistencia de eventos en TimescaleDB (PostgreSQL + series de tiempo)

| Campo | Valor |
|---|---|
| Estado | Aceptado |
| Fecha | 2026-10-09 |
| Decisores | Equipo de arquitectura del proyecto |
| Relacionados | ADR-001, ADR-007, ADR-008, ADR-010 |

## Contexto

El sistema guarda dos tipos de datos muy distintos:

1. **Eventos de sensores:** gran volumen (≈ 1,2 millones diarios en operación normal), escritura
   continua, casi nunca se modifican y se consultan **por rangos de tiempo** ("eventos de la Zona 4
   en los últimos 15 minutos", "eventos por minuto").
2. **Alertas, auditoría, lista de placas y SLO:** volumen bajo, con relaciones y actualizaciones
   (un operador valida una alerta). Requieren integridad transaccional.

El stack de referencia propone PostgreSQL para lo estructurado y Elasticsearch para el histórico.
Grafana debe consultar ambos y existe un requisito de retención: los datos se borran o anonimizan
pasado cierto tiempo (ADR-007).

### Dimensionamiento

- **Operación normal:** 13,5 eventos/s, unos 1,17 millones de filas diarias.
- **Tamaño:** ≈ 400 bytes por fila más índices, unos 0,8 GB diarios sin comprimir.
- **Con 90 días de retención y compresión a partir del día 45:** ≈ 36 GB sin comprimir más ≈ 4 GB
  comprimidos. Cabe con holgura en el disco de 1 TB del nodo A.

## Opciones consideradas

| Criterio | PostgreSQL puro | TimescaleDB (extensión de PostgreSQL) | InfluxDB | MongoDB / Elasticsearch (NoSQL) |
|---|---|---|---|---|
| Consultas por rango de tiempo | Buenas con índices; se degradan con cientos de millones de filas | Excelentes: particiona por tiempo automáticamente (*hypertables*) | Excelentes | Buenas (Elastic) / regulares (Mongo) |
| Datos relacionales y transacciones (alertas, auditoría) | Sí | Sí: es PostgreSQL | No (sin JOIN ni transacciones) | Limitadas |
| Compresión | No nativa | Nativa por columnas (≈ 90 % menos) | Nativa | Parcial |
| Retención automática | Manual | Política nativa (`add_retention_policy`) | Nativa | Manual (Elastic ILM) |
| Agregados precalculados | Vistas materializadas manuales | Agregados continuos que se actualizan solos | Tareas | Agregaciones al consultar |
| Lenguaje | SQL | SQL estándar más funciones de tiempo | Flux/InfluxQL (otro lenguaje) | Propio |
| Integración con Grafana | Nativa | Nativa (modo TimescaleDB) | Nativa | Nativa |
| Bases que operar | 1 | **1** | 2 (Influx + PostgreSQL para alertas) | 2 |
| Recursos | Bajos | Bajos | Bajos | Elastic ≈ 2 GB de RAM mínimo |

## Decisión

Se adopta **TimescaleDB 2.17 sobre PostgreSQL 16** como **única** base de datos:

- **`sensor_events` es una hypertable** con particiones (*chunks*) diarias, índices por zona, tipo y
  placa seudonimizada, compresión a partir de los 45 días y borrado automático a los 90 días.
- **Agregado continuo `events_per_minute`**, con agregación en tiempo real: Grafana grafica millones
  de eventos sin recorrerlos uno por uno.
- **Tablas relacionales normales** para `alerts`, `alert_revisions`, `audit_log`, `plate_watchlist`,
  `slo_measurements` y `rule_quality`. Su volumen es bajo y necesitan actualizaciones y llaves primarias.
- **Vistas `v_events` y `v_alerts`** sin seudónimos, para el usuario de solo lectura de Grafana (ADR-007).
- **Esquema versionado con Flyway** desde el código Java (`alert-service/src/main/resources/db/migration`).

**Se descarta Elasticsearch.** La búsqueda histórica que se necesita (por zona, tipo, placa
seudonimizada y rango de fechas) se resuelve con índices de TimescaleDB. Elasticsearch duplicaría
los datos, consumiría unos 2 GB más de RAM y sumaría una tercera base que operar, a cambio de
búsqueda de texto libre que hoy solo aplicaría al campo "mensaje" de los reportes ciudadanos. Si en el
futuro se necesita, se puede alimentar desde Kafka con un nuevo grupo de consumidores sin tocar nada
de lo existente.

## Consecuencias

**Positivas**
- **Una sola base de datos:** un solo respaldo, un solo motor que aprender, un solo usuario de
  Grafana. Los eventos y las alertas se pueden cruzar con SQL.
- La retención y la compresión son políticas declarativas: no hay que programar tareas de borrado
  de eventos.
- Las inserciones son por lotes e idempotentes (`ON CONFLICT DO NOTHING`): si Kafka reentrega un
  lote, no se duplican filas.
- Medido en la simulación: en el pico de 270 eventos/s, los datos aparecen en Grafana en 47 ms
  (p95 de frescura).

**Negativas (aceptadas)**
- Las funciones avanzadas (compresión y agregados continuos) usan la licencia Timescale License
  (TSL). Es gratuita para uso propio, pero no permite ofrecer TimescaleDB como servicio a terceros.
  Esto no afecta a una municipalidad.
- Los agregados continuos no se pueden crear dentro de una transacción: esa migración de Flyway se
  marca como no transaccional (`V2__agregados_continuos.sql.conf`).
- No hay búsqueda de texto libre avanzada (ver arriba).

## Impacto en el negocio

- **Costo:** sin licencias y sin un servidor adicional para Elasticsearch, lo que ahorra unos 2 GB de
  RAM y su administración. Con la compresión, el disco del nodo A alcanza para años.
- **Riesgo:** PostgreSQL es una base madura de más de 25 años, con amplia disponibilidad de personal
  en Guatemala. Con el borrado automático de datos viejos, se cumple la política de retención sin
  depender de que alguien lo recuerde.
- **Tiempo:** el esquema se crea solo al arrancar el sistema, sin pasos manuales.
