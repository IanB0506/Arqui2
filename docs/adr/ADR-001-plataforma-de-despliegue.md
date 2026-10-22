# ADR-001: Plataforma de despliegue on-premise (LAN municipal)

| Campo | Valor |
|---|---|
| Estado | Aceptado |
| Fecha | 2026-10-09 |
| Decisores | Equipo de arquitectura del proyecto |
| Relacionados | ADR-002, ADR-003, ADR-005, ADR-007 |

## Contexto

El enunciado exige elegir y defender un modelo de despliegue para un tipo de cliente concreto. Para
este proyecto se definió el siguiente **cliente hipotético**:

- **Municipalidad de tamaño medio** del área metropolitana de Guatemala: unos 500 000 habitantes,
  12 zonas y un centro de monitoreo municipal que opera 24/7 junto a la Policía Municipal.
- **Presupuesto de inversión limitado** y una partida de funcionamiento anual difícil de aumentar.
  Comprometer un gasto mensual en dólares que varía con el uso es un riesgo financiero y
  administrativo.
- **Sin personal especializado en nube.** Sí cuenta con un área de informática que administra
  servidores Linux.
- **Datos personales sensibles:** las placas vehiculares, la ubicación de ciudadanos y los reportes
  identifican a personas. El área legal exige que estos datos no salgan de la infraestructura
  municipal (ver ADR-007).
- **Continuidad operativa:** la detección debe funcionar aunque falle el enlace a internet. El
  enunciado pide "evitar depender de latencias externas no controladas para la parte de ingesta".
- **Volumen moderado:** unos 13,5 eventos por segundo en operación normal, con picos de 10 a 20
  veces en conciertos, partidos o feriados (unos 270 eventos/s).

Opciones del enunciado: on-premise/LAN, híbrido (LAN + nube) y cloud-native.

## Opciones consideradas

| Criterio | On-premise / LAN | Híbrido (ingesta local + analítica en nube) | Cloud-native |
|---|---|---|---|
| Costo inicial | Alto (hardware ≈ US$12 000) | Medio (≈ US$7 000 de hardware local) | Bajo (casi nulo) |
| Costo recurrente | Bajo y predecible (≈ US$7 500/año) | Medio y variable (nube + salida de datos) | Alto y variable (≈ US$11 000–14 000/año + enlace dedicado) |
| Control de los datos personales | Total: nunca salen de la red municipal | Parcial: el histórico se va a la nube | Bajo: todo en un proveedor externo |
| Latencia sensor → alerta | Muy baja: todo en LAN (medido: p50 ≈ 50 ms) | Baja para alertas (locales), alta para histórico | Depende de internet (+60 a 120 ms por tramo hacia la región más cercana) |
| Funciona sin internet | Sí | Las alertas sí, la analítica no | No |
| Elasticidad ante picos | No: se dimensiona para el pico | Parcial | Sí, automática |
| Complejidad operativa | Media: hay que administrar servidores | Alta: dos entornos y su integración | Media: servicios gestionados, pero hace falta experiencia en nube |
| Dependencia del proveedor (lock-in) | Ninguna (todo es software libre) | Media | Alta (MSK, RDS, ElastiCache…) |

### Estimación de costo (orden de magnitud, en US$, a validar con cotizaciones)

**On-premise (opción elegida):**

| Concepto | Detalle | Costo |
|---|---|---|
| Nodo A (datos) | Servidor de 8 núcleos, 32 GB de RAM, 2 × 1 TB SSD en RAID 1 | 3 500 |
| Nodo B (servicios y visualización) | Servidor de 8 núcleos, 32 GB de RAM | 2 500 |
| Nodos C/D (borde / pasarela de sensores) | 2 mini PC | 1 200 |
| Red | Switch administrable con VLAN y firewall/UTM pequeño | 1 500 |
| Energía | UPS de 3 kVA y rack | 1 800 |
| Contingencia | 15 % | 1 575 |
| **Inversión inicial (CAPEX)** | | **≈ 12 000 (≈ Q93 000)** |
| Energía | ≈ 800 W promedio, unos 7 000 kWh/año | 1 750/año |
| Mantenimiento y garantías | ≈ 10 % del CAPEX | 1 200/año |
| Administración | 0,25 de un puesto de técnico | 4 500/año |
| Licencias | Todo el software es libre | 0 |
| **Costo anual (OPEX)** | | **≈ 7 500/año** |
| **Costo total a 3 años (TCO)** | | **≈ 34 500** |

**Cloud-native** (referencia con precios de lista aproximados de un proveedor público):
- Kafka gestionado con 3 brokers: ≈ US$460/mes.
- PostgreSQL gestionado con alta disponibilidad: ≈ US$240/mes. TimescaleDB gestionado cuesta más.
- Redis gestionado: ≈ US$50/mes.
- Cómputo para 4 servicios: ≈ US$150/mes.
- VPN sitio a sitio y almacenamiento: ≈ US$100/mes.

Esto da ≈ **US$1 000/mes, unos US$12 000/año**. A eso hay que sumar un enlace de internet dedicado
con SLA, imprescindible porque todos los sensores dependerían de él: ≈ US$4 000–7 000/año.
**TCO a 3 años ≈ US$48 000–57 000**, sin inversión inicial.

**Híbrido:** ≈ US$7 000 de hardware local más ≈ US$400/mes de nube para histórico y analítica,
más la salida de datos. **TCO a 3 años ≈ US$36 000–40 000**, con la complejidad de dos entornos.

### Latencia extremo a extremo esperada (on-premise)

| Tramo | Estimado | Medido en la simulación |
|---|---|---|
| Sensor → Kafka (LAN) | < 5 ms | — |
| Ingesta (validar, normalizar, seudonimizar) | 5–20 ms | p95 de procesamiento < 10 ms |
| Correlación (consultas a Redis en memoria) | 5–30 ms | — |
| Persistencia y aviso a la consola | 10–50 ms | — |
| **Total evento → alerta guardada** | **≈ 50–150 ms** | **Operación normal: p50 ≈ 45–80 ms. Pico de diseño (270 eventos/s, multi-nodo): p50 = 39 ms, p95 = 67 ms, máximo = 80 ms** |

El objetivo de negocio (alerta en menos de 2 s, ADR-008) se cumple con mucho margen.

## Decisión

Se adopta un despliegue **on-premise sobre la LAN municipal**, repartido en los nodos de la sección 6
del enunciado:

- **Nodo A:** Kafka, TimescaleDB y Redis.
- **Nodo B:** microservicios, Prometheus y Grafana.
- **Nodos C/D:** generación de eventos (en producción serían las pasarelas de sensores) e
  instancias adicionales de procesamiento para repartir la carga.

Se diseña para el pico (×20) y no para el promedio. Como evolución futura queda la opción
**híbrida**: enviar a la nube solo agregados anónimos (conteos por zona y hora) para analítica
regional. Así se aprovecha la nube sin sacar datos personales.

## Consecuencias

**Positivas**
- Los datos personales nunca salen de la red municipal, lo que facilita la defensa legal (ADR-007).
- La detección funciona aunque se caiga internet.
- El costo recurrente es predecible y bajo, sin facturas variables en dólares.
- Latencia mínima: en el pico de diseño se midió p95 = 67 ms, unas 30 veces por debajo del objetivo de 2 s.
- No hay dependencia de un proveedor de nube; todo el stack es software libre.

**Negativas (aceptadas)**
- **No hay elasticidad.** Si un evento extraordinario supera ×20 el tráfico normal, el sistema se
  degrada. Se mitiga con:
  - degradación controlada (ADR-008);
  - escalado horizontal agregando instancias en los nodos C/D (probado en la simulación: Kafka
    reparte las 12 particiones entre máquinas automáticamente).
- **Inversión inicial y responsabilidad de operar el hardware:** energía, respaldos y reemplazo de
  discos.
- La alta disponibilidad real requiere duplicar el nodo A (3 brokers de Kafka y una réplica de
  PostgreSQL). En la simulación se acepta un solo nodo A (ver ADR-006).

## Impacto en el negocio

- **Costo:** una inversión única de unos Q93 000 y unos Q58 000 al año de funcionamiento. A 3 años
  sale entre un 30 % y un 40 % más barato que la nube, y el gasto es predecible para el presupuesto
  municipal.
- **Riesgo:** se elimina el riesgo de que los datos de placas y ubicaciones queden en manos de
  terceros, y el de quedar "ciego" si falla internet. A cambio, la municipalidad asume el riesgo de
  operar sus servidores, mitigado con UPS, discos en espejo y respaldos diarios.
- **Tiempo de implementación:** unas 6 a 8 semanas, incluida la compra de equipo. El sistema ya
  funciona en contenedores, así que la instalación toma horas una vez que el hardware está en sitio.
