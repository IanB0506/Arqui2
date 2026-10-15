# Architecture Decision Records (ADR)

Registro de las decisiones de arquitectura de **Centinela Urbano**. Cada ADR sigue el formato
obligatorio de la sección 4.1 del enunciado: **contexto, opciones consideradas (mínimo dos, con sus
trade-offs), decisión, consecuencias e impacto en el negocio** (redactado para un directivo no técnico).

| # | Decisión | Tema exigido (sección 4.2) |
|---|---|---|
| [ADR-001](ADR-001-plataforma-de-despliegue.md) | Despliegue **on-premise** en la LAN municipal, con costo estimado y latencia extremo a extremo | Plataforma de despliegue |
| [ADR-002](ADR-002-broker-de-eventos.md) | **Apache Kafka** (KRaft), un topic por sensor y 12 particiones | Broker de eventos |
| [ADR-003](ADR-003-persistencia-de-eventos.md) | **TimescaleDB** como única base de datos; se omite Elasticsearch | Persistencia de eventos |
| [ADR-004](ADR-004-orquestacion-de-tareas-periodicas.md) | **Spring Scheduler + ShedLock** en lugar de Airflow | Orquestación de tareas periódicas |
| [ADR-005](ADR-005-contenerizacion-y-orquestacion.md) | **Docker Compose** por nodo; decisión consciente de no usar Kubernetes | Contenerización y orquestación |
| [ADR-006](ADR-006-resiliencia-y-tolerancia-a-fallos.md) | Escritura anticipada en disco, reintentos según la clase de falla, DLQ con reproceso, vigilante del nodo B y política de pérdida aceptable | Resiliencia y tolerancia a fallos |
| [ADR-007](ADR-007-seguridad-y-privacidad-de-datos.md) | Seudonimización HMAC, retención de 30 días, mínimo privilegio y segmentación | Seguridad y privacidad de datos |
| [ADR-008](ADR-008-observabilidad-y-slos.md) | 4 SLO medibles y degradación controlada automática | Observabilidad y SLOs |
| [ADR-009](ADR-009-modelo-de-correlacion.md) | Motor de evidencia ponderada con ventanas en Redis | (adicional) Modelo de correlación |
| [ADR-010](ADR-010-estilo-arquitectonico-y-stack.md) | Microservicios orientados a eventos en Java 21 / Spring Boot | (adicional) Estilo arquitectónico y stack |

El enunciado exige al menos seis ADRs; aquí hay diez. Las ocho decisiones obligatorias tienen cada
una su propio ADR, y se suman dos más que justifican el modelo de inferencia y el stack. Todos los
ADRs se consolidan en el anexo del informe final (`informe/Anexo-A-ADRs.docx`).
