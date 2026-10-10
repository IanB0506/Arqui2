# Manual de instalación, operación y demo

## 1. Requisitos

- **Docker** (Docker Desktop en Windows/macOS) con Docker Compose v2.
- 8 GB de RAM libres para correr todo en una sola máquina.
- Opcional, para compilar fuera de Docker: **Java 21** y **Maven 3.9**.

## 2. Arranque rápido (una sola máquina)

```bash
git clone <repositorio> && cd Arqui2
cp .env.example .env          # ajustar contraseñas y la llave HMAC
docker compose up -d --build  # compila dentro de Docker; la primera vez tarda unos minutos
```

Si ya se compiló con Maven (`mvn package -DskipTests`), se puede usar `BUILD_TARGET=prebuilt` en
`.env` para construir las imágenes en segundos y sin internet.

| Servicio | URL | Usuario |
|---|---|---|
| Consola del operador | http://localhost:8083 | operador / operador123 (admin / admin123) |
| Grafana | http://localhost:3000 | admin / admin123 |
| API del simulador | http://localhost:8080/api/simulator/status | sin autenticación (red interna) |
| Prometheus | http://localhost:9090 | — |

Para el despliegue en varias máquinas, ver [`deploy/README.md`](../deploy/README.md).

## 3. Guion para la puesta en simulación (unos 15 minutos)

Cada paso corresponde a un criterio de evaluación de la sección 10.2.

### 3.1 Despliegue de la arquitectura elegida (5 %)

1. Mostrar el esquemático de infraestructura (`docs/img/infraestructura.png`) y explicar por qué es
   on-premise (ADR-001).
2. En el nodo A: `docker compose ps`. Kafka, TimescaleDB y Redis aparecen *healthy*.
3. En el nodo B: abrir http://IP_B:9090/targets. Todos los servicios aparecen UP.
4. Mostrar el reparto de carga entre máquinas:
   ```bash
   docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
     --bootstrap-server localhost:9092 --describe --group correlation --members
   ```
   Se ve `correlation-engine-nodo-b` y `correlation-engine-nodo-c` con 6 particiones cada uno.

### 3.2 Flujo de eventos y procesamiento en tiempo real (5 %)

1. Abrir Grafana → *Centro de Monitoreo - Tiempo real*: eventos por segundo y por tipo.
2. Lanzar un pico de carga y mostrar que la latencia no se mueve:
   ```bash
   curl -X POST "http://IP_C:8080/api/simulator/burst?multiplier=20&seconds=60"
   ```
3. En *SLOs y Salud del Sistema*: throughput por tipo de sensor, lag en 0 y p95 muy por debajo de 2 s.
4. Opcional: inyectar un mensaje inválido y mostrar la DLQ.
   ```bash
   echo '{json roto' | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
     --bootstrap-server localhost:9092 --topic sensor.panic.raw
   docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
     --topic events.dlq --from-beginning --timeout-ms 5000
   ```

### 3.3 Validación de correlaciones esperadas (5 %)

1. Abrir la consola del operador (http://IP_B:8083) en pantalla grande.
2. Lanzar el ejemplo exacto de la tabla 2.4:
   ```bash
   curl -X POST http://IP_C:8080/api/scenarios/tabla-2-4
   ```
3. Explicar lo que aparece en vivo:
   - a los ~12 s, **ROBO_EN_CURSO CRITICAL** en la Zona 4: pánico (0,40) + placa XYZ*** a alta
     velocidad (0,20) + 85 km/h (0,30) = 0,90. Con el escenario `robo-en-curso`, que envía primero la
     velocidad, se ve cómo escala de HIGH a CRITICAL;
   - a los ~90 s, **POSIBLE_FALSO_POSITIVO** en la Zona 10.
   - Abrir "Evidencia" y leer el puntaje: así se explica la inferencia.
4. Validación automática completa (unos 2,5 minutos):
   ```bash
   curl -X POST http://IP_C:8080/api/validation
   curl http://IP_C:8080/api/validation/latest      # "status": "PASSED", 9/9
   ```
5. Validar una alerta como operador: "Confirmar incidente" o "Marcar falso positivo". Explicar que
   eso alimenta el ajuste adaptativo de umbrales (dashboard *Análisis de Falsos Positivos*).

### 3.4 Presentación de resultados en Grafana (5 %)

Recorrer los tres dashboards:

1. **Centro de Monitoreo:** mapa de calor de eventos de riesgo con alertas por severidad; tabla de
   alertas recientes; eventos por minuto; alertas por tipo y zona.
2. **SLOs y Salud:** cumplimiento del SLO-1 (% < 2 s); p50/p95/p99; frescura; throughput; lag;
   latencia por etapa; modo degradado.
3. **Análisis de Falsos Positivos:** porcentaje de posibles falsos positivos; validaciones; calidad
   por regla y zona con su ajuste de umbral.

### Plan B

Si la red del laboratorio falla, todo el sistema corre en una sola laptop con `docker compose up -d`
(modo de una sola máquina), con los mismos dashboards y escenarios.

## 4. Referencia de la API

### sensor-simulator (puerto 8080)

| Método | Ruta | Descripción |
|---|---|---|
| GET | `/api/simulator/status` | Nodo, zonas, tasas, pico activo, eventos en spool |
| POST | `/api/simulator/baseline?enabled=false` | Apaga o enciende el tráfico de fondo |
| POST | `/api/simulator/burst?multiplier=20&seconds=60` | Pico de carga |
| GET | `/api/scenarios` | Lista de escenarios |
| POST | `/api/scenarios/{id}?zone=4&speed=1` | Lanza un escenario (`robo-en-curso`, `falso-positivo`, `tabla-2-4`, `accidente-vial`, `disparos`, `incendio`, `vehiculo-robado`, `boton-abusivo`, `emergencia-medica`, `pico-de-carga`) |
| POST | `/api/replay?resource=noche-de-viernes.csv&speed=2&jitter=true` | Replay CSV con aleatorización |
| POST | `/api/validation?resetState=true` | Validación automática de correlaciones |
| GET | `/api/validation/latest` | Resultado de la última validación |

### ingestion-service (puerto 8081): gateway de la app móvil

| Método | Ruta | Descripción |
|---|---|---|
| POST | `/api/v1/panic` | Botón de pánico desde la app (contrato `PanicButtonEvent`) |
| POST | `/api/v1/citizen-reports` | Reporte ciudadano |

### alert-service (puerto 8083, HTTP Basic)

| Método | Ruta | Rol | Descripción |
|---|---|---|---|
| GET | `/api/alerts?since=&zoneId=&type=&status=&limit=` | OPERATOR | Alertas |
| GET | `/api/alerts/{id}` | OPERATOR | Detalle con evidencia |
| PATCH | `/api/alerts/{id}/validation` | OPERATOR | `{"resolution":"CONFIRMED"|"FALSE_POSITIVE","notes":"..."}` |
| GET | `/api/alerts/stream` | OPERATOR | Alertas en vivo (Server-Sent Events) |
| GET | `/api/stats/summary?minutes=60` | OPERATOR | Resumen para la consola |
| GET | `/api/zones` | OPERATOR | Catálogo de zonas |
| GET/POST/DELETE | `/api/watchlist` | ADMIN (para modificar) | Lista de placas robadas (se guarda seudonimizada) |
| POST | `/api/admin/jobs/{rule-quality|slo|plate-retention|alert-expiry|watchlist}` | ADMIN | Ejecuta una tarea periódica |
| POST | `/api/admin/correlation/reset` | ADMIN | Limpia el estado temporal del motor (auditado) |

Todas las aplicaciones exponen `/actuator/health` y `/actuator/prometheus`.

## 5. Configuración

Todas las propiedades se pueden sobreescribir con variables de entorno (ver `.env.example`):

| Variable | Servicio | Uso |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | todos | Dirección de Kafka |
| `REDIS_HOST`, `REDIS_PASSWORD` | ingesta, correlación, alertas | Redis |
| `DB_HOST`, `DB_PASSWORD` | alertas | TimescaleDB |
| `PLATE_HMAC_SECRET` | ingesta, alertas | Llave de seudonimización (**igual en ambos**) |
| `SIMULATOR_ZONES`, `SIMULATOR_NODE_NAME` | simulador | Zonas que simula cada nodo |
| `CORRELATION_FP_TIMEOUT` | correlación | Plazo de corroboración (90 s por defecto) |
| `INGESTION_ALLOW_HISTORICAL` | ingesta | Acepta eventos con más de 24 h (replays históricos) |
| `NODE_NAME` | servicios | Etiqueta del nodo en métricas y en el client-id de Kafka |

Las reglas de correlación se editan en `correlation-engine/src/main/resources/application.yml`.

## 6. Compilar y probar

```bash
mvn install                 # compila los 5 módulos y ejecuta las 26 pruebas
mvn -pl correlation-engine test -Dtest=CorrelationEngineTest   # solo el modelo de correlación
```

## 7. Solución de problemas

| Síntoma | Causa probable | Solución |
|---|---|---|
| Los servicios no se conectan a Kafka en multi-nodo | `NODE_A_IP` incorrecta o firewall | Verificar `.env` y que el puerto 9092 esté abierto desde la LAN |
| Grafana muestra "No data" | Aún no hay datos o el reloj del navegador está desfasado | Esperar 1 minuto; revisar el rango de tiempo |
| El mapa de Grafana no tiene fondo | Sin internet no cargan los mosaicos del mapa | El mapa de calor y los puntos se dibujan igual |
| La validación da menos de 9/9 | Se corrió con `resetState=false` y había incidentes abiertos | Usar el valor por defecto (`resetState=true`) |
| `spoolPending` crece | Kafka no está disponible | Revisar el nodo A; los eventos críticos se reenvían solos al volver |
| Alerta `SLO_INCUMPLIDO` | Sobrecarga | Levantar workers: `docker compose --profile workers up -d` en el nodo C/D |
