# ADR-005: Contenerización con Docker y orquestación con Docker Compose por nodo (sin Kubernetes)

| Campo | Valor |
|---|---|
| Estado | Aceptado |
| Fecha | 2026-10-09 |
| Decisores | Equipo de arquitectura del proyecto |
| Relacionados | ADR-001, ADR-002, ADR-006 |

## Contexto

El sistema tiene 9 componentes: 4 microservicios Java, Kafka, TimescaleDB, Redis, Prometheus y
Grafana. Se reparten entre 3 o 4 máquinas del laboratorio con sistemas operativos distintos (Ubuntu,
macOS, Windows), según la tabla de nodos del enunciado. Para la puesta en simulación hay que
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
  `docker save` / `docker load` (ver `deploy/README.md`).

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
- La IP de los nodos se configura a mano en `.env` y en los objetivos de Prometheus.

## Impacto en el negocio

- **Costo:** sin licencias ni hardware extra para un plano de control. Se aprovechan las máquinas que
  ya existen.
- **Riesgo:** baja el riesgo operativo de la demo y del día a día, porque hay menos piezas y es más
  fácil diagnosticar. El riesgo de no tener alta disponibilidad automática se acepta para la escala
  actual y tiene definido su disparador de revisión.
- **Tiempo:** de horas a minutos para poner en marcha cada máquina. El personal municipal no necesita
  formación en Kubernetes.
