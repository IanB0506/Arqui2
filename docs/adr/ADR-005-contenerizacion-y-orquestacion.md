# ADR-005: Contenerización con Docker y orquestación con Docker Compose por nodo (sin Kubernetes)

| Campo | Valor |
|---|---|
| Estado | Aceptado |
| Fecha | 2026-10-09 |
| Decisores | Equipo de arquitectura del proyecto |
| Relacionados | ADR-001, ADR-002, ADR-006 |

## Contexto

El sistema tiene 9 componentes: 4 microservicios Java, Kafka, TimescaleDB, Redis, Prometheus y
Grafana. Se reparten entre 3 o 4 máquinas del laboratorio. La tabla de nodos del enunciado
*recomienda* Ubuntu para el nodo A, macOS/Linux para el B y Windows/Linux para C/D, pero las
computadoras disponibles del grupo son Windows 10/11, y el requisito del equipo es poder levantar todo
desde Windows sin usar Linux. Para la puesta en simulación hay que
levantarlos de forma repetible, con la misma configuración en todos los equipos, y el grupo debe
poder repartir la carga entre varias máquinas. El enunciado pide defender tanto la adopción de
Kubernetes como la **decisión consciente de no usarlo**.

## Opciones consideradas

| Criterio | Instalación directa (sin contenedores) | Docker Compose por nodo | Kubernetes (k3s / kubeadm) |
|---|---|---|---|
| Portabilidad Windows/macOS/Linux | Baja: cada SO se instala distinto | Alta: misma imagen en todos | Alta |
| Tiempo de puesta en marcha | Horas por máquina | Minutos (`docker compose up`) | Horas: montar el clúster, la red (CNI) y el almacenamiento |
| Escalado | Manual | Manual: se agregan instancias en otro nodo (*profile* `workers`) | Automático (HPA) |
| Recuperación ante fallas | Manual | `restart: unless-stopped` en el mismo host | Reprograma los pods en otro nodo |
| Curva de aprendizaje | Baja | Baja | Alta (pods, services, ingress, PVC, RBAC) |
| Consumo extra | Ninguno | Mínimo | Plano de control: ≈ 1–2 GB de RAM por nodo de control |
| Red entre máquinas | Configuración manual | Puertos publicados en la LAN | Red superpuesta (*overlay*); delicada en redes de laboratorio |
| Adecuado para 3–4 máquinas y 9 componentes | No | **Sí** | Sobredimensionado |

## Decisión

Se usa **Docker** para empaquetar cada componente y **Docker Compose** para orquestarlos, con dos modos:

1. **Una sola máquina** (`docker-compose.yml` en la raíz): todo el sistema con un comando. Sirve para
   desarrollo y como plan B en la demo.
2. **Multi-nodo** (`deploy/nodo-a`, `deploy/nodo-b`, `deploy/nodo-c`): un archivo Compose por rol. Los
   nodos se comunican por IP en la LAN; la variable `NODE_A_IP` indica dónde están Kafka, TimescaleDB
   y Redis.

Detalles:

- **Un único `Dockerfile` multi-etapa** para los 4 servicios (`--build-arg MODULE=...`):
  - `runtime` compila con Maven dentro de Docker, así que solo hace falta Docker instalado;
  - `prebuilt` usa los `.jar` ya compilados y no necesita internet en el laboratorio.
- **Imágenes sin privilegios:** usuario `centinela`, no root.
- **Repartir la carga:** `docker compose --profile workers up -d` en los nodos C/D levanta instancias
  extra de ingesta y correlación; Kafka les reparte las 12 particiones. Probado: 6 particiones por nodo
  y reasignación automática al apagar uno.
- **Llevar las imágenes al laboratorio:** se construyen antes y se transportan con
  `docker save` / `docker load` (`centinela.cmd exportar-imagenes` / `importar-imagenes`).
- **Operación desde Windows:** `centinela.cmd` (PowerShell 5.1/7, sin bash ni WSL a mano) configura
  cada PC (detecta la IP, reparte zonas, genera la llave HMAC), revisa los puertos que Windows reserva
  para Hyper-V/WSL y elige otros libres, abre el firewall con una sola elevación, levanta los nodos en
  orden esperando a que estén sanos y muestra el estado de toda la red. Los puertos publicados son
  variables de `.env`, iguales en todas las PCs.
- **SO de cada nodo:** como todo corre en contenedores, el mismo archivo Compose funciona en Windows
  (Docker Desktop), Linux o macOS. En el **laboratorio** se usa Windows 10/11 en todas las PCs, porque
  son las máquinas disponibles; Docker Desktop administra internamente su propia máquina virtual y el
  equipo no instala ni opera Linux. En **producción** los servidores usan Ubuntu Server 22.04 con
  Docker Engine, como recomienda el enunciado (ADR-001). La elección no cambia la arquitectura ni los
  archivos Compose.

**¿Por qué no Kubernetes?**
- Su principal valor, reprogramar contenedores automáticamente y escalar según la carga, rinde a
  partir de decenas de nodos o servicios.
- Con 3 o 4 máquinas, su plano de control consumiría una parte importante de la RAM disponible.
- Su red superpuesta añade puntos de falla justo el día de la demo en vivo.
- El escalado que este sistema necesita (agregar consumidores) ya lo resuelve Kafka, sin un orquestador.

**Cuándo migrar a Kubernetes:**
- más de unas 10 máquinas;
- requisitos de alta disponibilidad con reprogramación automática entre nodos;
- despliegues frecuentes sin cortes de servicio (*rolling updates*);
- o un modelo híbrido o en la nube (ADR-001, evolución futura).

Las imágenes ya son compatibles con Kubernetes, así que la migración no exige cambios de código.

## Consecuencias

**Positivas**
- Cualquier integrante del grupo levanta el sistema completo en su laptop con un comando.
- La misma imagen corre en desarrollo, en el laboratorio y en "producción".
- La configuración queda como código, versionada en Git.

**Negativas (aceptadas)**
- No hay reprogramación automática entre máquinas: si un nodo se apaga, sus contenedores no se
  mueven solos a otro. En la práctica se mitiga porque Kafka reasigna el trabajo de los consumidores
  a los que siguen vivos (probado).
- Todas las PCs deben compartir el mismo `.env` (IP, puertos y secretos). El script lo genera en la
  primera PC y detecta el rol de las demás por su IP, pero hay que copiar el archivo a mano.
- En Windows, Docker Desktop tiene particularidades que el script cubre: puertos reservados por
  Hyper-V/WSL (error 10013), firewall de entrada, memoria de la VM y reloj de la VM tras suspender.

## Impacto en el negocio

- **Costo:** sin hardware extra para un plano de control. En producción, Linux con Docker Engine no
  paga licencias; Docker Desktop, que se usa en el laboratorio, es gratuito para educación pero una
  organización del tamaño de una municipalidad pagaría suscripción, otra razón para usar Linux en los
  servidores. Se aprovechan las máquinas que ya existen.
- **Riesgo:** baja el riesgo operativo de la demo y del día a día, porque hay menos piezas y es más
  fácil diagnosticar. El riesgo de no tener alta disponibilidad automática se acepta para la escala
  actual y tiene definido su disparador de revisión.
- **Tiempo:** de horas a minutos para poner en marcha cada máquina. El personal municipal no necesita
  formación en Kubernetes.
