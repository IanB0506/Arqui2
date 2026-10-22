# Manual de instalación, operación y demo

Todo se instala, se levanta y se opera **desde Windows**, con `centinela.cmd` (un lanzador de
PowerShell que no requiere instalar nada más que Docker Desktop). No se usa Linux, WSL a mano ni
Bash: Docker Desktop corre los contenedores por su cuenta.

## 1. Requisitos

- **Windows 10/11** de 64 bits con **Docker Desktop** (motor WSL 2, que el instalador configura solo).
- 8 GB de RAM libres para correr todo en una sola PC; si se reparte, 8 GB en los nodos A y B y 4 GB
  en C/D (`centinela.cmd requisitos` revisa la memoria asignada a Docker).
- No hace falta Java ni Maven: `centinela.cmd compilar` compila dentro de Docker. (Si se prefiere
  compilar fuera, el proyecto trae Maven Wrapper: `mvnw.cmd install`, con Java 21.)
- El repositorio **fuera de OneDrive** de preferencia (OneDrive bloquea archivos mientras sincroniza).

`centinela.cmd requisitos` revisa todo lo anterior y dice qué falta.

## 2. Arranque rápido (una sola PC)

Desde la carpeta del proyecto, en *Símbolo del sistema* o PowerShell (o con doble clic en
`centinela.cmd`, que abre un menú):

```bat
centinela.cmd configurar        :: elige "una sola PC" y genera .env (con una llave HMAC propia)
centinela.cmd iniciar -Compilar :: compila (la primera vez tarda unos minutos) y levanta todo
centinela.cmd estado            :: salud de cada servicio
centinela.cmd abrir             :: consola del operador, Grafana y el vigilante en el navegador
```

| Servicio | URL | Usuario |
|---|---|---|
| Consola del operador | http://localhost:8083 | operador / operador123 (admin / admin123) |
| Grafana | http://localhost:3000 | admin / admin123 |
| Vigilante del nodo B | http://localhost:8080/vigilante.html | sin autenticación (red interna) |
| API del simulador | http://localhost:8080/api/simulator/status | sin autenticación (red interna) |
| Prometheus | http://localhost:9090 | — |

`configurar` genera una llave HMAC propia, pero las contraseñas quedan las de `.env.example`:
cámbielas en `.env` antes de la demo. Los puertos son los de `.env` (`ALERT_SERVICE_PORT`, `GRAFANA_PORT`, etc.). Si se cambiaron, las
URLs cambian igual; `centinela.cmd abrir` siempre usa los correctos.

### Error "Se intentó acceder a un socket de una forma no permitida por sus permisos de acceso"

Es el error 10013 de Windows: Hyper-V o WinNAT **reservan rangos de puertos** (el 8081 cae seguido
dentro de uno) y Docker no puede publicarlos. Solución sin tocar Windows:

```bat
centinela.cmd puertos -Arreglar   :: detecta los puertos reservados u ocupados y elige otros libres
centinela.cmd iniciar
```

Para liberar el puerto original (opcional, en una consola de administrador):
`netsh interface ipv4 show excludedportrange protocol=tcp` muestra los rangos;
`net stop winnat` y `net start winnat` los vuelve a repartir.

## 3. Despliegue en varias PCs

La guía completa está en [`deploy/README.md`](../deploy/README.md). En resumen, en **cada** PC:

```bat
centinela.cmd configurar -Nodo A        :: (o B, C, D) con la IP de A y de B
centinela.cmd firewall                  :: abre solo los puertos de ese nodo (pide administrador)
centinela.cmd iniciar                   :: en orden: primero A, luego B, luego C/D
```

El mismo `.env` sirve para todas las PCs (se copia en USB). `centinela.cmd estado` en cualquier PC
dice si alcanza a las demás.

## 4. Guion para la puesta en simulación (unos 15 minutos)

Cada paso corresponde a un criterio de evaluación de la sección 10.2.

### 4.1 Despliegue de la arquitectura elegida (5 %)

1. Mostrar el esquemático de infraestructura (`docs/img/infraestructura.png`) y explicar por qué es
   on-premise (ADR-001).
2. `centinela.cmd estado` en el nodo A: Kafka, TimescaleDB y Redis aparecen *healthy*.
3. En el nodo B: abrir http://IP_B:9090/targets. Todos los servicios aparecen UP.
4. `centinela.cmd particiones`: seis consumidores `correlation-engine-nodo-b-*` y seis
   `correlation-engine-nodo-c-*`, con una partición cada uno: 6 + 6 por máquina (Kafka reparte la
   carga entre máquinas).

### 4.2 Flujo de eventos y procesamiento en tiempo real (5 %)

1. Abrir Grafana → *Centro de Monitoreo - Tiempo real*: eventos por segundo y por tipo.
2. `centinela.cmd pico -Multiplicador 20 -Segundos 60` y mostrar que la latencia no se mueve.
3. En *SLOs y Salud del Sistema*: throughput por tipo de sensor, lag en 0 y p95 muy por debajo de 2 s.
4. Opcional: `centinela.cmd dlq` muestra la cola de errores con la clase de cada falla.

### 4.3 Validación de correlaciones esperadas (5 %)

1. Abrir la consola del operador (http://IP_B:8083) en pantalla grande.
2. `centinela.cmd escenario tabla-2-4` (el ejemplo exacto de la tabla 2.4).
3. Explicar lo que aparece en vivo:
   - a los ~12 s, **ROBO_EN_CURSO CRITICAL** en la Zona 4: pánico (0,40) + placa XYZ*** a alta
     velocidad (0,20) + 85 km/h (0,30) = 0,90. Con `escenario robo-en-curso`, que envía primero la
     velocidad, se ve cómo escala de HIGH a CRITICAL;
   - a los ~90 s, **POSIBLE_FALSO_POSITIVO** en la Zona 10;
   - abrir "Evidencia" y leer el puntaje: así se explica la inferencia.
4. `centinela.cmd validar`: validación automática completa (~2,5 min), termina en **9/9 PASSED**.
5. Validar una alerta como operador ("Confirmar incidente" o "Marcar falso positivo"): alimenta el
   ajuste adaptativo de umbrales (dashboard *Análisis de Falsos Positivos*).

### 4.4 Presentación de resultados en Grafana (5 %)

1. **Centro de Monitoreo:** mapa de calor de eventos de riesgo, alertas por severidad, tabla de
   alertas recientes, eventos por minuto, alertas por tipo y zona.
2. **SLOs y Salud:** cumplimiento del SLO-1 (% < 2 s), p50/p95/p99, frescura, throughput, lag,
   latencia por etapa, modo degradado y eventos en spool (críticos y telemetría).
3. **Análisis de Falsos Positivos:** porcentaje de posibles falsos positivos, validaciones, calidad
   por regla y zona con su ajuste de umbral.

### 4.5 Tolerancia a fallos (opcional, muy demostrativo)

| Paso | Comando | Qué se ve |
|---|---|---|
| 1 | Abrir la consola y `http://IP_C:8080/vigilante.html` | Todo OPERATIVO |
| 2 | `centinela.cmd caida B` (en la PC de B) | En ~16 s el vigilante marca **NODO B SIN RESPUESTA** y dice dónde sigue la detección (worker de C). La consola muestra "Sin conexión… reintentando" |
| 3 | `centinela.cmd escenario disparos` | El worker de C genera DISPAROS_CONFIRMADOS aunque B esté caído |
| 4 | `centinela.cmd recuperar B` | La consola dice "Conexión restablecida tras N s: X alertas…" y las destaca con "LLEGÓ N s TARDE" |
| 5 | `centinela.cmd caida A`, `escenario robo-en-curso`, `recuperar A` | El simulador guarda el pánico y la telemetría útil en disco; al volver A los reenvía y la alerta sale como **ROBO_EN_CURSO**, no como falso positivo |

### Plan B

Si la red del laboratorio falla, todo el sistema corre en una sola laptop (`centinela.cmd configurar`
→ una sola PC), con los mismos dashboards y escenarios. Sin internet, las imágenes se llevan en USB:
`centinela.cmd exportar-imagenes` en una PC con internet e `importar-imagenes` en la del laboratorio.

## 5. Comandos de `centinela.cmd`

| Grupo | Comando | Qué hace |
|---|---|---|
| Preparar | `configurar [-Nodo UNICO\|A\|B\|C\|D]` | Asistente: roles de esta PC, IPs, workers y llave HMAC → `.env` |
| | `requisitos` | Docker Desktop, memoria de la VM, relojes, OneDrive |
| | `puertos [-Arreglar]` | Revisa puertos reservados (10013) u ocupados; con `-Arreglar` elige otros |
| | `firewall [-Quitar]` | Abre (o cierra) en el firewall los puertos de este nodo |
| | `compilar` / `pruebas [modulo]` | Maven dentro de Docker / pruebas automatizadas |
| Operar | `iniciar [-Compilar] [-Nodo X]` | Levanta lo que corre en esta PC, en orden, y espera a que esté sano |
| | `detener [-Borrar]` | Detiene esta PC (con `-Borrar` elimina los datos, pide confirmación) |
| | `estado` | Contenedores, salud, vigilante, conectividad entre PCs y relojes |
| | `logs [servicio]` / `abrir` | Registros / navegador |
| Demo | `escenarios`, `escenario <id> [-Zona] [-Velocidad]` | Escenarios de correlación |
| | `validar`, `pico`, `fondo on\|off`, `replay` | Validación 9/9, carga, tráfico de fondo, CSV |
| | `caida <A\|B\|C>`, `recuperar <A\|B\|C>` | Simula la caída de un nodo y lo vuelve a encender |
| | `particiones` | Reparto de particiones entre instancias |
| | `dlq`, `reprocesar-dlq [-IncluirPermanentes]` | Cola de errores y su reproceso |
| | `tarea <nombre>`, `reiniciar-correlacion` | Tareas periódicas a demanda; limpiar el estado del motor |
| Datos | `respaldo`, `restaurar [archivo]` | Respaldo de la base (además del automático cada 6 h) |
| | `exportar-imagenes`, `importar-imagenes` | Imágenes en USB para laboratorios sin internet |

## 6. Referencia de la API

### sensor-simulator (puerto 8080)

| Método | Ruta | Descripción |
|---|---|---|
| GET | `/api/simulator/status` | Nodo, zonas, tasas, pico activo, pendientes en spool (críticos y telemetría), estado del nodo B |
| POST | `/api/simulator/baseline?enabled=false` | Apaga o enciende el tráfico de fondo |
| POST | `/api/simulator/burst?multiplier=20&seconds=60` | Pico de carga |
| GET | `/api/scenarios` | Lista de escenarios |
| POST | `/api/scenarios/{id}?zone=4&speed=1` | Lanza un escenario (`robo-en-curso`, `falso-positivo`, `tabla-2-4`, `accidente-vial`, `disparos`, `incendio`, `vehiculo-robado`, `boton-abusivo`, `emergencia-medica`, `pico-de-carga`) |
| POST | `/api/replay?resource=noche-de-viernes.csv&speed=2&jitter=true` | Replay CSV con aleatorización |
| POST | `/api/validation?resetState=true` | Validación automática de correlaciones |
| GET | `/api/validation/latest` | Resultado de la última validación |
| GET | `/api/watchdog` | Vigilante: estado del nodo B (OPERATIVO / DEGRADADO / SIN_RESPUESTA), cada servicio y cada grupo de consumidores de Kafka |
| GET | `/vigilante.html` | Página del vigilante (se refresca sola) |
| GET | `/api/simulator/spool` | Detalle del spool en disco (pendientes, reenviados, apartados, últimos problemas) |
| POST | `/api/simulator/spool/resend` | Fuerza una ronda de reenvío |
| POST | `/api/simulator/spool/rejected/requeue` | Devuelve a la cola los eventos apartados por error permanente |

### ingestion-service (puerto 8081): gateway de la app móvil

| Método | Ruta | Descripción |
|---|---|---|
| POST | `/api/v1/panic` | Botón de pánico desde la app (contrato `PanicButtonEvent`) |
| POST | `/api/v1/citizen-reports` | Reporte ciudadano |
| GET | `/api/v1/gateway/status` | Kafka disponible, pendientes en el spool del gateway, reenviados |

Respuestas del gateway: **202 `RECIBIDO`** (Kafka lo confirmó), **202 `EN_COLA`** (Kafka no respondió
a tiempo; quedó guardado en disco con `fsync` y se reenvía solo), **503 `NO_DISPONIBLE`** con
`Retry-After: 10` (el disco del spool está lleno o falla: la app debe reintentar) y **400** si el
evento es inválido.

### alert-service (puerto 8083, HTTP Basic)

| Método | Ruta | Rol | Descripción |
|---|---|---|---|
| GET | `/api/alerts?since=&zoneId=&type=&status=&limit=` | OPERATOR | Alertas |
| GET | `/api/alerts/{id}` | OPERATOR | Detalle con evidencia |
| PATCH | `/api/alerts/{id}/validation` | OPERATOR | `{"resolution":"CONFIRMED"|"FALSE_POSITIVE","notes":"..."}` |
| GET | `/api/alerts/stream` | OPERATOR | Alertas en vivo (Server-Sent Events) |
| GET | `/api/stats/summary?minutes=60` | OPERATOR | Resumen para la consola |
| GET | `/api/zones` | OPERATOR | Catálogo de zonas |
| GET | `/api/console/config` | OPERATOR | Configuración de la consola (puerto de Grafana) |
| GET/POST/DELETE | `/api/watchlist` | ADMIN (para modificar) | Lista de placas robadas (se guarda seudonimizada) |
| POST | `/api/admin/jobs/{rule-quality|slo|plate-retention|alert-expiry|watchlist}` | ADMIN | Ejecuta una tarea periódica |
| POST | `/api/admin/correlation/reset` | ADMIN | Limpia el estado temporal del motor (auditado) |
| GET | `/api/admin/dlq?ultimos=15` | ADMIN | Resumen de la DLQ: total por clase de falla y últimos mensajes |
| POST | `/api/admin/dlq/reprocess?incluirPermanentes=false` | ADMIN | Reinyecta la DLQ en su topic original (auditado) |

Todas las aplicaciones exponen `/actuator/health`, `/actuator/health/liveness` (responde aunque
Redis o Kafka estén caídos: el proceso vive) y `/actuator/prometheus`.

## 7. Configuración

**Variables de `.env`** (las lee `docker compose` o `centinela.cmd`; ver `.env.example`). El mismo
`.env` sirve para todas las PCs:

| Variable | Servicio | Uso |
|---|---|---|
| `NODE_A_IP`, `NODE_B_IP` | todos | IPs de los servidores en la LAN |
| `GENERATOR_IPS` | nodo B, `centinela.cmd` | IP del nodo C (y del D si existe): Prometheus y reparto de zonas |
| `*_PORT` (`INGESTION_PORT`, `ALERT_SERVICE_PORT`, …) | todos | Puertos publicados en Windows (ver `centinela.cmd puertos`) |
| `REDIS_PASSWORD`, `DB_PASSWORD` | ingesta, correlación, alertas | Contraseñas |
| `PLATE_HMAC_SECRET` | ingesta, alertas | Llave de seudonimización (**igual en todas las PCs**) |
| `BUILD_TARGET` | imágenes | `prebuilt` usa los jar ya compilados (segundos, sin internet) |
| `WORKERS` | nodo C | `true` levanta workers de ingesta y correlación en C |
| `BACKUP_INTERVAL_HOURS`, `BACKUP_KEEP` | respaldo | Cada cuánto se respalda la base (6 h) y cuántos se guardan (8) |

Las zonas de cada generador (`SIMULATOR_ZONES`) las calcula `centinela.cmd` según `GENERATOR_IPS`:
con una IP, C simula las 12; con dos, C simula 1-6 y D 7-12.

**Propiedades avanzadas** (se cambian en el `application.yml` del servicio y se vuelve a compilar
con `centinela.cmd iniciar -Compilar`):

| Propiedad | Servicio | Valor por defecto |
|---|---|---|
| `correlation.false-positive-timeout` | correlación | Plazo de corroboración: 90 s |
| `ingestion.allow-historical` | ingesta | Rechaza eventos con más de 24 h (activar para replays históricos) |
| `simulator.spool.telemetry-max-age`, `telemetry-max-size` | simulador | Telemetría útil en disco: 15 min / 32 MB |
| `simulator.spool.critical-max-size` | simulador | Spool de críticos: 32 MB (≈ 100 000 eventos) |
| `simulator.watchdog.timeout`, `interval-ms`, `failures-to-down` | simulador | Vigilante: 3 s de espera, cada 10 s, 2 fallas para declarar caído |
| `alerts.slo.*` | alertas | SLO: 2 s en el 95 %, frescura 10 s, sobrecarga con atraso ≥ 1 000 sostenido (`overload-lag`) |
| Reglas de correlación (`correlation.rules`) | correlación | Ver [`correlacion.md`](correlacion.md) |

## 8. Compilar y probar

```bat
centinela.cmd compilar                     :: compila los 5 módulos dentro de Docker
centinela.cmd pruebas                      :: las 95 pruebas automatizadas
centinela.cmd pruebas correlation-engine   :: solo el modelo de correlación
```

Con Java 21 instalado también sirve `mvnw.cmd install` (descarga Maven la primera vez).

## 9. Solución de problemas

| Síntoma | Causa probable | Solución |
|---|---|---|
| "...no permitida por sus permisos de acceso" al iniciar | Puerto reservado por Hyper-V/WinNAT (error 10013) | `centinela.cmd puertos -Arreglar` |
| Los servicios de B o C no alcanzan a Kafka | `NODE_A_IP` incorrecta o firewall de A | `centinela.cmd estado` en B; `centinela.cmd firewall` en A |
| Grafana muestra "No data" | Aún no hay datos o reloj desfasado | Esperar 1 minuto; `centinela.cmd requisitos` revisa los relojes |
| El mapa de Grafana no tiene fondo de calles | Sin internet no cargan los mosaicos de OpenStreetMap | Las zonas, sus nombres, el mapa de calor y las alertas se dibujan igual con la capa local |
| La validación da menos de 9/9 | Se corrió con `resetState=false` con incidentes abiertos | Usar `centinela.cmd validar` (reinicia el estado) |
| `spoolPending` crece | Kafka (nodo A) no está disponible | Revisar el nodo A; los críticos y la telemetría útil se reenvían solos al volver |
| Consola: "Sin conexión con el servidor" | alert-service (nodo B) caído o red | El vigilante (`/vigilante.html`) dice qué cayó; al volver, la consola recupera lo perdido |
| Alerta `SLO_INCUMPLIDO` HIGH ("Degradación del servicio") | Sobrecarga real: atraso grande que no baja | Levantar workers en C/D (`WORKERS=true` y `centinela.cmd iniciar`) |
| Alerta `SLO_INCUMPLIDO` MEDIUM ("Puesta al día tras una caída") | Alertas tardías por una caída ya superada | Ninguna: es informativa, no se activa el modo degradado |
| Mensajes en la DLQ | Eventos inválidos (PERMANENTE) o errores no clasificados (DESCONOCIDO); las caídas de Redis o la base no llegan a la DLQ | `centinela.cmd dlq`; los DESCONOCIDO se reprocesan solos cada 5 min (hasta 3 veces) |
