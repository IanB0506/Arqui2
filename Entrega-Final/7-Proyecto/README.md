# Centinela Urbano

Microservicios orientados a eventos en Java 21 y Spring Boot 3.5, sobre Apache Kafka, TimescaleDB,
Redis, Prometheus y Grafana. Se despliega en una sola PC o repartido en los nodos A, B y C/D.

```
common/              contratos de eventos, zonas, seudonimización y diario en disco (librería)
sensor-simulator/    simulador de los cinco sensores, escenarios, replay CSV, validación y vigilante
ingestion-service/   validación, normalización, privacidad, deduplicación, DLQ y puerta HTTP de la app
correlation-engine/  motor de correlación (reglas en application.yml, editables en infra/correlation)
alert-service/       persistencia, API, consola del operador, tareas periódicas y migraciones Flyway
infra/               Prometheus, Grafana (fuentes de datos y tableros), reglas editables y respaldos
deploy/              un docker-compose por nodo: A datos, B servicios, C/D generadores y workers
docker-compose.yml   todo en una sola PC
centinela.cmd/.ps1   instalación y operación desde Windows
```

## Puesta en marcha en una PC

Solo se necesita Docker Desktop:

```bat
centinela.cmd configurar        :: opción 1: todo en esta PC
centinela.cmd iniciar -Compilar :: compila dentro de Docker y levanta todo
centinela.cmd validar           :: comprueba las 9 correlaciones esperadas
```

Sin el script, en cualquier sistema: copiar `.env.example` como `.env` y ejecutar
`docker compose up -d --build`.

El manual completo y la guía de despliegue en varias PCs están en la carpeta
`6-Manual-de-operacion` de la entrega.
