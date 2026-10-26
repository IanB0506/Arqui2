# ADR-004: Tareas periódicas con Spring Scheduler + ShedLock (en lugar de Apache Airflow)

| Campo | Valor |
|---|---|
| Estado | Aceptado |
| Fecha | 2026-10-09 |
| Decisores | Equipo de arquitectura del proyecto |
| Relacionados | ADR-003, ADR-007, ADR-008, ADR-009, ADR-010 |

## Contexto

El stack de referencia asigna a Apache Airflow las "tareas periódicas de análisis, validación y
entrenamiento". El enunciado pregunta explícitamente: **¿se justifica la complejidad operativa de
Airflow a esta escala?** Las tareas que necesita el sistema son:

| Tarea | Frecuencia | Qué hace |
|---|---|---|
| Monitor de SLO | Cada 30 s | Mide la latencia y la frescura de datos y activa la degradación controlada (ADR-008) |
| Retención de placas | Diaria (02:15) | Borra seudónimos de placas de más de 30 días (ADR-007) |
| Expiración de alertas | Cada 5 min | Pasa a EXPIRED las alertas que nadie atendió en 2 h |
| Calidad de reglas | Cada hora | Calcula la precisión por regla y zona y ajusta los umbrales del motor (ADR-009) |
| Sincronización de la lista de placas | Cada 10 min | Copia los seudónimos de PostgreSQL a Redis |
| Revisión de la lista de placas | Cada 15 s | Si Redis se reinició vacío, recarga la lista de inmediato (ADR-006) |
| Reproceso de la DLQ | Cada 5 min | Reinyecta lo que falló por causas no permanentes, hasta 3 veces (ADR-006) |

Son **7 tareas independientes, cortas (de milisegundos a segundos) y sin dependencias entre sí**. No
hay flujos de varios pasos, reintentos encadenados ni reprocesos de fechas pasadas (*backfill*).
Consideración adicional: el equipo decidió escribir todo el sistema en Java (ADR-010), y Airflow solo
acepta flujos (DAGs) en Python, lo que sumaría un segundo lenguaje y un segundo entorno que mantener.

## Opciones consideradas

| Criterio | Apache Airflow | Quartz Scheduler en clúster | Spring `@Scheduled` + ShedLock |
|---|---|---|---|
| Para qué está pensado | Flujos de datos complejos con dependencias (DAGs) | Planificación avanzada (calendarios, persistencia de disparos) | Tareas periódicas simples dentro de la aplicación |
| Componentes extra | Webserver, scheduler, workers, base de metadatos y, opcionalmente, Redis/Celery | Tablas de Quartz en la base de datos | Una tabla (`shedlock`) |
| RAM adicional | ≈ 2–4 GB | ≈ 0 (dentro de la JVM) | ≈ 0 (dentro de la JVM) |
| Lenguaje de las tareas | **Python** (un segundo lenguaje en un sistema escrito todo en Java, ADR-010) | Java | Java |
| Interfaz para ver ejecuciones | Excelente | No | No (se usa la auditoría, los logs y Grafana) |
| Evitar ejecuciones duplicadas con varias instancias | Sí | Sí (clúster) | Sí (bloqueo distribuido en la base de datos) |
| Curva de aprendizaje | Alta | Media | Baja (anotaciones) |

## Decisión

Se usa **Spring `@Scheduled` con ShedLock (proveedor JDBC sobre TimescaleDB)** dentro del
`alert-service`.

- Cada tarea se declara con `@Scheduled` (cron o intervalo) y `@SchedulerLock(name=...)`.
- **ShedLock garantiza que cada tarea corra en una sola instancia**, aunque haya varias copias del
  `alert-service` en distintas máquinas. Usa la hora de la base de datos (`usingDbTime()`), así que
  no le afectan relojes desincronizados entre nodos.
- Para la demo, las tareas se pueden disparar manualmente con `POST /api/admin/jobs/{tarea}` (rol ADMIN).
- Las acciones que tocan datos personales (purga de placas) quedan en la bitácora `audit_log`.

**Cuándo sí convendría Airflow:** si el proyecto creciera hacia el entrenamiento real de modelos de
aprendizaje automático, con flujos de varios pasos (extraer → limpiar → entrenar → validar →
publicar), reprocesos de meses anteriores o decenas de tareas con dependencias. Esa revisión queda
registrada como el disparador para reconsiderar esta decisión.

## Consecuencias

**Positivas**
- No hay que operar 4 a 5 contenedores más ni reservar 2 a 4 GB de RAM.
- Todo el código del sistema queda en Java y se prueba con las mismas herramientas.
- La tarea de calidad de reglas cumple el rol de "análisis y entrenamiento" del enunciado: aprende
  de las validaciones de los operadores y ajusta los umbrales del motor.

**Negativas (aceptadas)**
- No hay una interfaz gráfica de ejecuciones como la de Airflow. Se compensa con logs, la bitácora de
  auditoría y las tablas `slo_measurements` y `rule_quality` visibles en Grafana.
- Si el `alert-service` está caído, sus tareas no corren. Con varias instancias, cualquiera de ellas
  toma el bloqueo.

## Impacto en el negocio

- **Costo:** se evita el equivalente a un servidor pequeño (2 a 4 GB de RAM) y la capacitación en
  Airflow.
- **Riesgo:** menos piezas significa menos puntos de falla. El riesgo de no tener Airflow es bajo,
  porque las tareas son simples, y la decisión indica cuándo revisarla.
- **Tiempo:** se implementó en horas en lugar de días; agregar una tarea nueva es escribir un método
  con una anotación.
