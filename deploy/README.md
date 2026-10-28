# Despliegue en varias máquinas (nodos A, B, C/D)

Esta carpeta reparte Centinela Urbano entre varias computadoras del laboratorio, siguiendo la
tabla de nodos del enunciado (sección 6). Para una sola máquina, usar el modo de una sola PC
(`centinela.cmd configurar` → opción 1, o el `docker-compose.yml` de la raíz).

| Nodo | Carpeta | Qué corre | SO | RAM mínima |
|---|---|---|---|---|
| A | `nodo-a/` | Kafka (KRaft), TimescaleDB, Redis, respaldo automático de la base | Windows 10/11 con Docker Desktop (el enunciado recomienda Ubuntu Server 22.04) | 8 GB |
| B | `nodo-b/` | ingestion-service, correlation-engine, alert-service, Prometheus, Grafana | Windows 10/11 con Docker Desktop (recomendado: macOS / Linux) | 8 GB |
| C | `nodo-c/` | sensor-simulator (zonas 1-6), vigilante + *workers* opcionales | Windows 10/11 con Docker Desktop | 4 GB |
| D | `nodo-c/` | sensor-simulator (zonas 7-12), vigilante + *workers* opcionales | Windows 10/11 con Docker Desktop | 4 GB |

El enunciado da el sistema operativo como **recomendado**, no obligatorio. Como todo corre en
contenedores (ADR-005), los mismos archivos funcionan en Windows, Linux o macOS (en producción se
usaría Ubuntu Server con Docker Engine, ADR-001). Esta guía usa Windows en todas las PCs del
laboratorio: solo hace falta **Docker Desktop** y una copia del repositorio. No se
usa bash, ni WSL a mano, ni ninguna máquina Linux (Docker Desktop administra internamente su propia
máquina virtual).

## 1. Preparar cada PC (una sola vez)

1. Instalar **Docker Desktop** y abrirlo hasta que diga *Engine running*.
2. Copiar el repositorio (git clone o ZIP). Conviene **fuera de OneDrive** (por ejemplo
   `C:\dev\Arqui2`), o marcar la carpeta como "Mantener siempre en este dispositivo".
3. Conectar todas las PCs a la misma red y comprobar que se ven (desde cmd: `ping <ip>`; si el ping
   falla pero lo demás funciona es normal, Windows bloquea el ping por defecto).
4. Desactivar la suspensión durante la demo: si una PC se duerme, su nodo se cae.

Todo lo demás lo hace `centinela.cmd`, que está en la raíz del repositorio. Se ejecuta con doble
clic (muestra un menú) o desde cmd/PowerShell con un comando (`centinela.cmd ayuda` los lista).

## 2. Configurar (en este orden)

**En la PC del nodo A** (la primera):

```bat
centinela.cmd configurar
```

- Elegir `2` (varias PCs) y escribir `A` como nodo de esta PC.
- El script detecta la IP de la PC y pregunta las IP de B, C y D (`ipconfig` las muestra).
- Genera una llave de seudonimización aleatoria, revisa que Windows deje usar los puertos
  (si alguno está reservado por Hyper-V/WSL, elige otro libre) y ofrece abrir el firewall
  (pide permiso de administrador una vez).
- Al terminar queda un archivo **`.env`** en la raíz del repositorio.

**Copiar ese `.env` a la raíz del repositorio de las otras PCs** (USB, carpeta compartida o correo).
Todas las PCs deben usar el **mismo** `.env`: las IP, los puertos y los secretos tienen que
coincidir.

**En cada una de las otras PCs:**

```bat
centinela.cmd configurar
```

Elija la opción 2 (varias PCs): el script sugiere qué nodo es la PC comparando su IP con las del
`.env` y usted lo confirma. Las zonas se reparten solas: con un solo generador, C simula las 12;
con dos, C simula 1-6 y D 7-12. En el nodo B escribe la lista de generadores para Prometheus.

> Si en alguna PC el script tuvo que cambiar un puerto, una IP o los *workers*, lo avisa: hay que
> volver a copiar ese `.env` a las demás PCs. Si una PC se probó antes en modo de una sola PC, el
> script recuerda que debe usar el `.env` de la primera PC (su propio `.env` tiene otra llave de
> seudonimización) y ofrece detener los contenedores del modo anterior.

## 3. Levantar

En cada PC, **primero A, luego B, luego C y D**:

```bat
centinela.cmd iniciar
```

El script espera a que el nodo quede sano. En B, C y D comprueba antes que llega a Kafka del nodo
A; si no llega, avisa (revise la IP de A en `.env`, que A esté iniciado y su firewall) y continúa,
porque los servicios reintentan solos.

La primera vez se construyen las imágenes con Maven dentro de Docker (necesita internet y unos
minutos). Si el `.env` copiado dice `BUILD_TARGET=prebuilt` pero esta PC no tiene los jar compilados,
el script lo detecta y construye con Maven igual. Si el laboratorio no tiene internet, ver la
sección 6.

Verificar desde cualquier PC:

```bat
centinela.cmd estado
```

Muestra los contenedores de esa PC, la salud de los tres nodos (Kafka, TimescaleDB, Redis,
servicios, Grafana, Prometheus, simulador) y lo que ve el **vigilante** de los nodos C/D.

- Consola del operador: `http://<IP_NODO_B>:8083` (operador / operador123)
- Grafana: `http://<IP_NODO_B>:3000` (admin / admin123)
- Vigilante: `http://<IP_NODO_C>:8080/vigilante.html`
- Prometheus: `http://<IP_NODO_B>:9090/targets` (todos los servicios deben estar "UP")

`centinela.cmd abrir` abre las tres primeras en el navegador.

## 4. Repartir la carga de procesamiento

Si en `configurar` se respondió que sí a los *workers*, los nodos C y D levantan también instancias
de ingesta y de correlación. Los topics tienen **12 particiones** y Kafka las reparte solas entre
todas las instancias de cada grupo de consumidores. Para verlo:

```bat
centinela.cmd particiones
```

La columna `CLIENT-ID` indica en qué nodo está cada consumidor: seis `correlation-engine-nodo-b-*`
y seis `correlation-engine-nodo-c-*`, con una partición cada uno (6 + 6 por máquina). Si un
worker se apaga, Kafka reasigna sus particiones a las instancias que quedan en unos segundos.

## 5. Demostrar la tolerancia a fallos

En la PC del nodo que se quiere tumbar:

```bat
centinela.cmd caida B
centinela.cmd recuperar B
```

Mientras B está caído, el **vigilante** de los nodos C/D lo marca como caído (y lo registra en su
log). Con los *workers* la detección sigue funcionando. Al volver, la consola recupera las alertas
que se perdió y marca las que llegaron tarde. El detalle de cada caso está en
[`docs/adr/ADR-006-resiliencia-y-tolerancia-a-fallos.md`](../docs/adr/ADR-006-resiliencia-y-tolerancia-a-fallos.md).

## 6. Laboratorio sin internet

En una PC con internet, desde la raíz del repositorio:

```bat
centinela.cmd exportar-imagenes
```

Se genera `centinela-imagenes.tar`. Copiarlo a cada PC del laboratorio y ejecutar:

```bat
centinela.cmd importar-imagenes
centinela.cmd iniciar
```

Use `iniciar` **sin** `-Compilar`: las imágenes ya están cargadas, y `-Compilar` intentaría
construirlas de nuevo con Maven, que sin internet falla.

## 7. Problemas frecuentes en Windows

| Síntoma | Causa | Solución |
|---|---|---|
| `ports are not available ... forbidden by its access permissions` | Hyper-V/WSL reservaron ese puerto (rango excluido de Windows) | `centinela.cmd puertos -Arreglar` elige otros libres. Arreglo permanente (cmd como administrador, luego reiniciar): `netsh int ipv4 set dynamic tcp start=49152 num=16384` |
| Grafana marca 0 eventos por segundo, o Prometheus (`http://IP_B:9090/targets`) muestra los objetivos del nodo C en DOWN con `context deadline exceeded` | Las PCs no se alcanzan: firewall, `.env` distinto entre PCs, perfil de red *Pública*, antivirus con firewall propio o Wi-Fi con aislamiento de clientes | Ejecutar `centinela.cmd estado` en **cada** PC: la **huella** del `.env` debe ser igual en todas (si no, copiar el `.env` de la primera PC y repetir `configurar`), y la lista de salud dice qué nodo no se alcanza desde esa PC. Luego `centinela.cmd firewall` en cada PC, red en perfil *Privada* y, si persiste, probar con cable o con un router propio |
| B o C no llegan al nodo A ("No llego a Kafka") | Firewall del nodo A, `.env` distinto entre PCs (otra IP o otro `KAFKA_PORT`), PCs en subredes distintas, red Wi-Fi con aislamiento de clientes | `iniciar` dice qué falla: si A responde en Redis o la base pero no en Kafka, Kafka no está levantado en A; si no responde en nada, es la red o el firewall de A. Luego: `centinela.cmd estado` en las 3 PCs (misma **huella**), `centinela.cmd firewall` en el nodo A (permite la subred local y las IP de los nodos del `.env`) y `Test-NetConnection IP_A -Port 9092` desde B |
| `Bind for 0.0.0.0:8082 failed: port is already allocated` (u 8081, 8083) al iniciar B | La PC fue antes otro nodo u otro modo (p. ej. el nodo C con workers, que publican 8081 y 8082) y esos contenedores siguen encendidos; Docker Desktop los vuelve a arrancar al abrirse | `iniciar` nombra el contenedor que tiene el puerto y ofrece detenerlo y reintentar. A mano: `docker ps -a` y `docker compose -p centinela-nodo-c down` (o el proyecto que aparezca: `centinela`, `centinela-nodo-a`, `centinela-nodo-b`) |
| `port is already allocated` y ningún contenedor usa ese puerto, o `puertos` dice "Docker Desktop lo retiene" | Docker Desktop quedó con el puerto tomado de un intento fallido | Reiniciar Docker Desktop (ícono junto al reloj > *Restart*) e `iniciar` otra vez. No cambiar el puerto: el `.env` debe ser el mismo en todas las PCs |
| `configurar` detectó la IP de la red equivocada (Wi-Fi y cable a la vez) | La PC tiene dos adaptadores | El script prefiere una IP de esta PC que ya figure en el `.env` y avisa si la IP de su nodo no es de esta PC; corrija la IP cuando la pregunta |
| Docker Desktop en modo de contenedores Windows | Se cambió el modo | El script lo detecta y lo dice: clic derecho en el ícono de Docker > *Switch to Linux containers* |
| Con C y D, se quiere apagar el tráfico de fondo de uno solo | — | `centinela.cmd fondo off -Nodo D`; sin `-Nodo`, `fondo` y `pico` actúan en los dos generadores |
| Se pulsó "Cancelar" en el aviso del firewall de Docker | Windows creó una regla que bloquea a Docker Desktop | `centinela.cmd firewall` la quita y abre los puertos del nodo |
| El nodo A se reinicia por falta de memoria | La máquina virtual de Docker tiene poca RAM | `centinela.cmd requisitos` muestra la memoria; subirla en `%UserProfile%\.wslconfig` (`[wsl2]` `memory=8GB`) y reiniciar Docker Desktop |
| La IP de una PC cambió (DHCP) | Kafka anuncia la IP vieja | Volver a ejecutar `centinela.cmd configurar` en todas las PCs con el `.env` corregido e `iniciar` |
| `centinela.cmd` no abre | Archivo bajado como ZIP y bloqueado | Clic derecho → Propiedades → Desbloquear, o usar git clone |

## Uso manual con docker compose (sin el script)

El script solo arma estos comandos. En cualquier sistema operativo, con el mismo `.env` en la raíz:

```bat
docker compose --env-file .env -f deploy/nodo-a/docker-compose.yml up -d
docker compose --env-file .env -f deploy/nodo-b/docker-compose.yml up -d
docker compose --env-file .env -f deploy/nodo-c/docker-compose.yml up -d
docker compose --env-file .env -f deploy/nodo-c/docker-compose.yml --profile workers up -d
```

En el nodo B, `deploy/nodo-b/targets/generadores.json` lista las IP de los generadores para
Prometheus (hay un ejemplo en `generadores.json.ejemplo`; el script lo genera solo). En el nodo D
hay que definir `SIMULATOR_ZONES=7,8,9,10,11,12` y `SIMULATOR_NODE_NAME=nodo-d`.

Puertos que deben quedar abiertos hacia la red local (ADR-007, segmentación):

| Destino | Puerto (variable en `.env`) | Quién se conecta | Para qué |
|---|---|---|---|
| Nodo A | 9092 (`KAFKA_PORT`) | B, C, D | Kafka |
| Nodo A | 5432 (`DB_PORT`) | B | TimescaleDB |
| Nodo A | 6379 (`REDIS_PORT`) | B, C, D | Redis |
| Nodo B | 8083 (`ALERT_SERVICE_PORT`) | Operadores, C, D | API, consola y validación |
| Nodo B | 8081, 8082 (`INGESTION_PORT`, `CORRELATION_PORT`) | C, D | Gateway de la app y vigilante |
| Nodo B | 3000 (`GRAFANA_PORT`), 9090 (`PROMETHEUS_PORT`) | Operadores | Grafana y Prometheus |
| Nodo C/D | 8080 (`SIMULATOR_PORT`), 8081/8082 (`WORKER_*_PORT`) | B | Métricas para Prometheus y control del simulador |
