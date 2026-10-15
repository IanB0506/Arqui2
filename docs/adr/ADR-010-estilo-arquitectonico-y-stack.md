# ADR-010: Microservicios orientados a eventos en Java 21 + Spring Boot (simulador incluido)

| Campo | Valor |
|---|---|
| Estado | Aceptado |
| Fecha | 2026-10-09 |
| Decisores | Equipo de arquitectura del proyecto |
| Relacionados | ADR-002, ADR-003, ADR-004, ADR-005 |

## Contexto

El objetivo general pide una solución basada en una **arquitectura distribuida**. El curso exige
**Java** como lenguaje y permite Spring. El enunciado sugiere scripts de Python, JMeter o la línea de
comandos de Kafka para simular eventos, y el stack de referencia incluye Elasticsearch y Airflow.
Además, el grupo quiere **poder ejecutar el sistema en más de una máquina a la vez para repartir la
carga**.

Hay que decidir:

1. cómo dividir el sistema en componentes;
2. con qué tecnología construirlos;
3. qué hacer con las herramientas sugeridas que no encajan.

## Opciones consideradas

| Criterio | Monolito modular (1 app Spring) + simulador | **Microservicios orientados a eventos (4 servicios)** | Microservicios por tipo de sensor (≈ 8 servicios) |
|---|---|---|---|
| "Arquitectura distribuida" del enunciado | Débil | **Sí** | Sí |
| Repartir la carga entre máquinas | No (todo o nada) | **Sí: cada servicio escala por separado** | Sí |
| Aislamiento de fallas | Bajo: una falla tumba todo | Alto | Alto |
| Complejidad operativa | Baja | Media (4 imágenes) | Alta (código duplicado) |
| Memoria | ≈ 0,5 GB | ≈ 1,6 GB | ≈ 3 GB o más |

| Simulador | Ventaja | Desventaja | Decisión |
|---|---|---|---|
| Scripts de Python | Rápido de escribir | Incumple "Java obligatorio"; duplica los contratos de eventos | Descartado |
| JMeter / Artillery | Bueno para estrés HTTP | No modela escenarios correlacionados (pánico → velocidad → fuga) | Opcional: el gateway REST de la app (`/api/v1/panic`) puede estresarse con JMeter si se desea |
| **Simulador en Java/Spring Boot** | Reutiliza los contratos de `common`; escenarios reproducibles; API de control para la demo | Más código | **Elegido** |

## Decisión

**Cuatro microservicios Spring Boot 3.5 sobre Java 21**, en un proyecto Maven de varios módulos, que
se comunican **solo por Kafka** (estilo orientado a eventos):

| Servicio | Responsabilidad | Escala con |
|---|---|---|
| `sensor-simulator` | Emula los 5 tipos de sensores: tráfico de fondo, 10 escenarios (incluida la tabla 2.4), picos de carga, replay CSV con aleatorización, buffer en disco para eventos críticos y validación automática | Una instancia por nodo generador, cada una con un subconjunto de zonas |
| `ingestion-service` | Valida, normaliza, seudonimiza, deduplica y envía a la DLQ; gateway REST para la app móvil | Instancias del grupo `ingestion` |
| `correlation-engine` | Reglas por evidencia ponderada, incidentes, falsos positivos, seguimiento de placas | Instancias del grupo `correlation` (estado en Redis) |
| `alert-service` | Persistencia en TimescaleDB, API REST, consola del operador en vivo (SSE), seguridad, tareas periódicas | Instancias detrás de un balanceador; las tareas usan ShedLock |
| `common` (librería) | Contratos de eventos con los campos del enunciado, catálogo de zonas, seudonimización | — |

Decisiones relacionadas con el stack de referencia:

- **Elasticsearch: se omite.** El histórico se busca en TimescaleDB (ADR-003).
- **Airflow: se reemplaza** por Spring Scheduler + ShedLock (ADR-004).
- **Se agrega Prometheus** para métricas técnicas (ADR-008).
- **Kafka, PostgreSQL (TimescaleDB), Redis y Grafana: se adoptan.**

Calidad del código:

- 90 pruebas unitarias; las del motor usan las reglas reales del YAML y las de resiliencia simulan
  cortes de luz, fallas de Kafka y muertes de proceso;
- configuración externa por variables de entorno;
- comentarios en español que explican el porqué de cada decisión.

## Consecuencias

**Positivas**
- Cumple el requisito de Java en el **100 % del código**, incluido el simulador.
- **Distribución real probada:** el motor de correlación corrió en dos máquinas lógicas a la vez con
  6 particiones cada una, y la validación dio 9 de 9 en modo multi-nodo.
- Cada servicio se puede desplegar, escalar y depurar por separado. Si la base de datos falla, la
  detección en tiempo real sigue funcionando.
- Spring Boot aporta salud (`/actuator/health`), métricas, seguridad y Kafka con poca configuración.

**Negativas (aceptadas)**
- Cuatro servicios Java suman unos 1,6 GB de RAM, frente a ≈ 0,5 GB de un monolito.
- Depurar un flujo que cruza servicios es más difícil; se mitiga con el `event_id` que viaja desde el
  sensor hasta la alerta.
- Hay que mantener los contratos de eventos compatibles entre servicios; se resuelve con el módulo
  `common`, compartido.

## Impacto en el negocio

- **Costo:** Java y Spring son libres y hay mucho personal disponible en Guatemala, lo que reduce el
  costo de mantenimiento a largo plazo.
- **Riesgo:** una falla en un componente no detiene todo el sistema. Elegir tecnologías de uso amplio
  reduce la dependencia de personas específicas.
- **Tiempo:** el proyecto Maven compila y prueba todo con un comando (`mvn install`), y las imágenes
  Docker se construyen igual para todos los servicios.
