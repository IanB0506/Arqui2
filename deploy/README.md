# Despliegue en varias máquinas (nodos A, B, C/D)

Esta carpeta reparte Centinela Urbano entre varias computadoras del laboratorio, siguiendo la
tabla de nodos del enunciado (sección 6). Para una sola máquina, usar el `docker-compose.yml` de la
raíz del repositorio.

| Nodo | Carpeta | Qué corre | SO recomendado | RAM mínima |
|---|---|---|---|---|
| A | `nodo-a/` | Kafka (KRaft), TimescaleDB, Redis | Ubuntu Server 22.04 | 8 GB |
| B | `nodo-b/` | ingestion-service, correlation-engine, alert-service, Prometheus, Grafana | macOS / Linux | 8 GB |
| C | `nodo-c/` | sensor-simulator (zonas 1-6) + *workers* opcionales | Windows / Linux | 4 GB |
| D | `nodo-c/` | sensor-simulator (zonas 7-12) + *workers* opcionales | Windows / Linux | 4 GB |

Todas las máquinas necesitan **Docker** (Docker Desktop en Windows/macOS) y una copia del repositorio.

## 1. Preparar la red

1. Conectar todas las máquinas al mismo switch o red del laboratorio y anotar la IP de cada una
   (`ip a` en Linux, `ipconfig` en Windows).
2. Abrir en el firewall **solo** estos puertos (ver ADR-007, segmentación):

| Destino | Puerto | Quién se conecta | Para qué |
|---|---|---|---|
| Nodo A | 9092 | B, C, D | Kafka |
| Nodo A | 5432 | B | TimescaleDB |
| Nodo A | 6379 | B, C, D | Redis |
| Nodo B | 8083 | Operadores, C, D | API y consola del operador |
| Nodo B | 3000 | Operadores | Grafana |
| Nodo C/D | 8080-8082 | B | Métricas para Prometheus y control del simulador |

## 2. Construir las imágenes (recomendado ANTES del día de la demo)

El laboratorio puede no tener internet. Construya las imágenes en casa y llévelas en una memoria USB:

```bash
# en una máquina con internet, desde la raíz del repositorio
docker compose build                       # compila con Maven dentro de Docker
docker pull apache/kafka:3.9.1 timescale/timescaledb:2.17.2-pg16 redis:7.4-alpine \
            grafana/grafana:11.4.0 prom/prometheus:v3.1.0
docker save -o centinela-imagenes.tar centinela/sensor-simulator:1.0.0 centinela/ingestion-service:1.0.0 \
  centinela/correlation-engine:1.0.0 centinela/alert-service:1.0.0 apache/kafka:3.9.1 \
  timescale/timescaledb:2.17.2-pg16 redis:7.4-alpine grafana/grafana:11.4.0 prom/prometheus:v3.1.0

# en cada máquina del laboratorio
docker load -i centinela-imagenes.tar
```

## 3. Levantar los nodos (en este orden)

En **cada** máquina, dentro de su carpeta:

```bash
cp ../../.env.example .env
# editar .env: NODE_A_IP, NODE_B_IP y los secretos (iguales en todos los nodos)
```

**Nodo A** (datos):
```bash
cd deploy/nodo-a && docker compose up -d --wait
```

**Nodo B** (servicios). Antes, editar `targets/generadores.json` con las IP de los nodos C y D:
```bash
cd deploy/nodo-b && docker compose up -d
```

**Nodo C** (`SIMULATOR_ZONES=1,2,3,4,5,6`, `SIMULATOR_NODE_NAME=nodo-c` en `.env`):
```bash
cd deploy/nodo-c && docker compose up -d
```

**Nodo D**: misma carpeta `nodo-c/`, con `SIMULATOR_ZONES=7,8,9,10,11,12` y `SIMULATOR_NODE_NAME=nodo-d`.

## 4. Repartir la carga de procesamiento

En los nodos C y D se pueden levantar instancias adicionales de ingesta y correlación:

```bash
docker compose --profile workers up -d
```

No hay que configurar nada más: los topics tienen **12 particiones** y Kafka las reparte
automáticamente entre todas las instancias de cada grupo de consumidores. Para comprobarlo:

```bash
# en el nodo A
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group correlation --members
```

La columna `CLIENT-ID` indica en qué nodo está cada consumidor (por ejemplo
`correlation-engine-nodo-b` y `correlation-engine-nodo-c`, con 6 particiones cada uno).
Si un worker se apaga, Kafka reasigna sus particiones a los que quedan en pocos segundos
(probado: la alerta se generó en 188 ms con el worker caído).

> Si una máquina corre a la vez el nodo B y los workers, cambie los puertos de los workers en su
> `.env` con `WORKER_INGESTION_PORT=9081` y `WORKER_CORRELATION_PORT=9082`.

## 5. Verificar

```bash
curl http://<IP_NODO_C>:8080/api/simulator/status        # el simulador genera eventos
curl -X POST http://<IP_NODO_C>:8080/api/validation      # validación automática (≈ 2,5 min)
curl http://<IP_NODO_C>:8080/api/validation/latest        # resultado: 9/9 esperado
```

- Consola del operador: `http://<IP_NODO_B>:8083` (operador / operador123)
- Grafana: `http://<IP_NODO_B>:3000` (admin / admin123)
- Prometheus: `http://<IP_NODO_B>:9090/targets` (todos los servicios deben estar "UP")
