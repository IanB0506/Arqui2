# Matriz de requisitos del enunciado

Cada requisito del enunciado, cómo lo cumple Centinela Urbano y dónde está la evidencia. Revisada el
25 de octubre de 2026, con el sistema en modo multi-nodo.

## Objetivo, alcance y simulación (secciones 1 y 2)

| Requisito | Cómo se cumple | Evidencia |
|---|---|---|
| Simulación funcional con detección, procesamiento, correlación y visualización sobre una arquitectura distribuida | 4 microservicios que solo se comunican por Kafka, repartibles entre 3 o 4 PCs | `docs/arquitectura.md`; ADR-010; informe, sección 5 |
| Decisiones defendibles ante actores no técnicos | Cada ADR cierra con "Impacto en el negocio" (costo, riesgo y tiempo) | `docs/adr/`; Anexo A |
| Sin dispositivos físicos: simulación lógica y programática | `sensor-simulator`: tráfico de fondo, 10 escenarios, picos, replay CSV | `sensor-simulator/`; informe, sección 3.2 |
| 2.1 Los 5 sensores con sus datos | Contratos con los campos del enunciado (botón de pánico, LPR, velocidad, acústico, reporte ciudadano) | `common/src/main/java/.../model/`; informe, Tabla 1 |
| 2.2 Herramientas sugeridas | CLI de Kafka (mensajes dañados a la DLQ) y CSV con aleatorización (`noche-de-viernes.csv`). En lugar de scripts de Python y JMeter, el simulador en Java (preferencia del equipo, defendida en el ADR-010) | `docs/resultados.md` 3.1; `sensor-simulator/src/main/resources/replay/` |
| 2.3 Eventos simultáneos y picos | Validación con 8 escenarios en paralelo; pico ×20 (270 eventos/s) y sobrecarga ×100 | Informe, Tablas 7 y 10 |
| 2.3 Correlacionar varias fuentes | 7 reglas de evidencia ponderada que combinan pánico, LPR, velocidad, acústicos y reportes | `docs/correlacion.md`; ADR-009 |
| 2.3 Integración, trazabilidad, visualización y decisiones con datos no estructurados | Evento normalizado común; texto libre como detalle de la evidencia; la consola abre el evento original; validación con un clic | Informe, Tabla 9 |
| 2.4 Ejemplo de la tabla 2.4 | Escenario `tabla-2-4`: fila 1 → ROBO_EN_CURSO CRITICAL; fila 2 → POSIBLE_FALSO_POSITIVO | `centinela.cmd escenario tabla-2-4`; informe, Figura 3 |

## Plataforma y ADRs (secciones 3 y 4)

| Requisito | Cómo se cumple | Evidencia |
|---|---|---|
| Elegir y defender LAN, híbrido o nube para un cliente hipotético declarado | On-premise para una municipalidad media del área metropolitana | ADR-001; informe, secciones 3.1 y 6.1 |
| Estimación de costo (orden de magnitud) | TCO a 3 años: ≈ US$34 500 on-premise, 36 000–40 000 híbrido, 48 000–57 000 nube | ADR-001; informe, Tabla 6 |
| Latencia esperada extremo a extremo | Estimada 20–105 ms; medida p50 44–78 ms y p95 67 ms en el pico | ADR-001; informe, sección 6.1 |
| La plataforma es el primer ADR | ADR-001 | `docs/adr/README.md` |
| 4.1 Formato: contexto, opciones (≥ 2), decisión, consecuencias, impacto en el negocio | Los 10 ADR tienen las 5 secciones | `docs/adr/` |
| 4.2 Las 8 decisiones obligatorias, mínimo 6 ADR | 10 ADR: los 8 obligatorios (001–008) más correlación (009) y estilo y stack (010) | `docs/adr/README.md` |
| ADRs consolidados en un anexo | `informe/Anexo-A-ADRs.docx` | `informe/` |

## Stack, nodos y casos de uso (secciones 5 a 7)

| Requisito | Cómo se cumple | Evidencia |
|---|---|---|
| Cada cambio al stack de referencia respaldado por un ADR | Kafka, PostgreSQL (TimescaleDB), Redis y Grafana adoptados; Elasticsearch omitido (ADR-003); Airflow reemplazado por Spring Scheduler y ShedLock (ADR-004); Prometheus agregado (ADR-008) | ADR-003, 004, 008 y 010 |
| Distribución por nodos A, B y C/D | Un Docker Compose por nodo y `centinela.cmd configurar` en cada PC | `deploy/`; informe, Tabla 2 |
| Botón de pánico de usuarios distribuidos | Botones físicos y de la app en las 12 zonas; `centinela.cmd panico` usa la puerta HTTP de la app | Manual, sección 5 |
| Reportes ciudadanos contextualizados | Texto libre y ubicación aproximada; `centinela.cmd reporte` | Manual, sección 5 |
| Correlaciones "posible robo en curso" y "accidente vial" | Reglas R1, R2 y R3 | `docs/correlacion.md` |
| Falsos positivos y validación | Ancla sin corroborar en 90 s → POSIBLE_FALSO_POSITIVO; el operador confirma o descarta; umbrales adaptativos por zona | Informe, secciones 5.4 y 7.3 |
| Mapas de calor, listas de incidentes y alertas críticas | Grafana (mapa de las zonas, alertas recientes) y consola en vivo | `infra/grafana/dashboards/`; informe, Figuras 4 a 7 |

## Entregables (sección 8)

| Entregable | Dónde |
|---|---|
| Diseño de infraestructura con nodos, servicios y segmentos de red | Informe, Figura 2 y sección 5.2; ADR-007 (VLAN y DMZ) |
| Documento de ADRs | `informe/Anexo-A-ADRs.docx` (fuente: `docs/adr/`) |
| Configuración de servicios y contenedores | `docker-compose.yml`, `deploy/nodo-*/docker-compose.yml`, `*/src/main/resources/application.yml`, `infra/` |
| Scripts de simulación de eventos | `sensor-simulator/` (escenarios, picos, replay CSV) y `centinela.cmd` |
| Documentación del sistema de correlación e inferencia | `docs/correlacion.md` |
| Visualización en Grafana | 3 tableros provisionados: centro de monitoreo, SLO y salud, falsos positivos |
| Informe final: arquitectura, justificación con referencia a los ADRs, evaluación de resultados y comparación con ciudades inteligentes | `informe/Informe-Final-Centinela-Urbano.docx`, secciones 5 a 8 |

## Consideraciones (sección 9)

| Requisito | Cómo se cumple |
|---|---|
| Sensores emulados por herramientas automatizadas | `sensor-simulator` |
| On-premise sin depender de latencias externas para la ingesta | Todo corre en la LAN; la detección funciona sin internet (incluido el mapa de Grafana) |
| Placas y ubicaciones tratadas según el ADR de privacidad, aunque sean sintéticas | Seudónimo HMAC, placa enmascarada, ubicación a ≈ 110 m, retención de 30 días, Grafana sin seudónimos (ADR-007) |
| Comprender correlación, tiempo real, arquitectura orientada a eventos, gobernanza y visualización | Informe, sección 4 (4.1 a 4.7) y guía de estudio |

## Calificación (sección 10) y formato (sección 11)

| Criterio | Preparación |
|---|---|
| 10.2 Despliegue de la arquitectura elegida | `centinela.cmd configurar` e `iniciar` en cada PC; guion en el manual, sección 4.1 |
| 10.2 Flujo de eventos y procesamiento en tiempo real | `centinela.cmd fondo`, consola en vivo y tablero de SLO; manual, sección 4.2 |
| 10.2 Validación de correlaciones esperadas | `centinela.cmd validar` (9 de 9) y `escenario tabla-2-4`; manual, sección 4.3 |
| 10.2 Presentación de resultados en Grafana | Manual, sección 4.4 |
| 10.3 Mérito: análisis de falsos positivos | Tablero de falsos positivos, ajuste adaptativo de umbrales y medición con el tráfico de fondo (informe, sección 7.3) |
| 11 Carta, Arial 12, interlineado 1.5, márgenes de 2,54 cm, sangría de 5 espacios, número arriba a la derecha sin numerar portada ni índice, APA 7 | Lo aplica el generador del informe, también en tablas y notas. Falta completar la portada (grupo, integrantes, carnés y catedrático) y exportar los PDF desde Word |
