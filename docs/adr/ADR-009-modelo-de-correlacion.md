# ADR-009: Modelo de correlación por evidencia ponderada con ventanas en Redis

| Campo | Valor |
|---|---|
| Estado | Aceptado |
| Fecha | 2026-10-09 |
| Decisores | Equipo de arquitectura del proyecto |
| Relacionados | ADR-002, ADR-004, ADR-006, ADR-008 |

## Contexto

El núcleo del proyecto es **correlacionar eventos de fuentes heterogéneas** para inferir incidentes,
como en la tabla 2.4 del enunciado:

- un botón de pánico en la Zona 4 a las 22:05, más la placa XYZ123 en una cámara, más un vehículo a
  85 km/h, significan un **robo en curso**;
- un botón de pánico en la Zona 10 sin ningún evento complementario es un **posible falso positivo**.

Requisitos:

- decidir en menos de 2 s (ADR-008);
- tolerar eventos que llegan desordenados;
- **explicar** por qué se generó cada alerta: el operador y un juez deben entenderla;
- ajustarse sin recompilar;
- funcionar con varias instancias en distintas máquinas;
- y contemplar falsos positivos (por ejemplo, la pirotecnia en Guatemala dispara sensores acústicos).

## Opciones consideradas

| Criterio | Reglas en código (if/else) | **Motor propio de evidencia ponderada, reglas en YAML, estado en Redis** | Kafka Streams (uniones con ventanas) | Motor CEP (Esper / Apache Flink) | Aprendizaje automático (clasificador) |
|---|---|---|---|---|---|
| Explicabilidad | Media | **Alta**: cada alerta lista su evidencia y su peso | Media | Media | Baja (caja negra) |
| Cambiar reglas sin recompilar | No | **Sí** (YAML) | No | Sí (EPL/SQL) | Reentrenar |
| Correlacionar más de 2 fuentes con ventanas flexibles | Difícil | Sí | Complejo (uniones encadenadas) | Sí | Sí |
| Estado compartido entre instancias | No | Sí (Redis) | Estado local por partición | Clúster propio | — |
| Datos etiquetados necesarios | No | No | No | No | **Sí: no existen** |
| Infraestructura extra | No | Redis (ya en el stack) | No | Clúster de Flink | Pipeline de entrenamiento |
| Curva de aprendizaje | Baja | Baja | Media-alta | Alta | Alta |

## Decisión

Se implementa un **motor de reglas de evidencia ponderada** (`correlation-engine`):

- **Regla = ancla + evidencias.**
  - El ancla es el evento que inicia la sospecha, por ejemplo un botón de pánico de tipo ROBO.
  - Las evidencias son condiciones con peso: exceso de velocidad (+0,30), vehículo a alta velocidad
    en una cámara LPR (+0,20), placa en la lista de robados (+0,20), disparo o vidrios rotos (+0,15),
    otro testigo (+0,10).
- **Puntaje** = peso del ancla + suma de las evidencias encontradas en la **misma zona** dentro de la
  ventana `[ancla − antes, ancla + después]`, con un máximo de 1,0.
  - Puntaje ≥ umbral (0,70): se genera la alerta.
  - Puntaje ≥ umbral crítico (0,90): la severidad es **CRITICAL**.
- **Una persona o un sensor no puede corroborarse a sí mismo:** dentro de cada evidencia, cada
  fuente cuenta una sola vez.
- **Ventanas deslizantes en Redis** (*sorted sets* por zona y tipo, con la hora como puntaje). Se
  evalúa cada vez que llega un evento, sea ancla o evidencia, por lo que el **desorden no importa**.
- **Incidentes:**
  - las correlaciones del mismo tipo en la misma zona durante 10 min forman **un solo incidente**;
  - se emite una revisión cuando sube la severidad o aparece la placa (ESCALATED) o cuando se suma un
    nuevo testigo (UPDATED). No se repite la alerta.
- **Falsos positivos:**
  - un ancla que no se corrobora en 90 s produce `POSIBLE_FALSO_POSITIVO`, con severidad baja;
  - un dispositivo que presiona pánico más de 5 veces en 10 min se marca como sospechoso y se ignora
    durante 1 h;
  - un disparo aislado (posible pirotecnia) no basta: hacen falta 2 señales.
- **Seguimiento entre zonas:** la placa de un robo queda "sospechosa" 30 min. Si aparece en otra
  zona, se genera `VEHICULO_EN_FUGA`, vinculada al incidente original.
- **Umbrales adaptativos:** la tarea horaria de calidad (ADR-004) calcula la precisión por regla y
  zona a partir de las validaciones de los operadores.
  - Si la precisión es menor al 50 %, se exige más evidencia en esa zona (+0,10 al umbral).
  - Si se omiten incidentes reales (falsos negativos), se relaja (−0,05).
- **Rendimiento:**
  - filtrado temprano: los eventos que no cumplen ninguna condición de ninguna regla (≈ 99 % de la
    telemetría normal) no se guardan ni se evalúan;
  - los comandos de cada ventana van a Redis en un solo viaje (pipeline).

Las 8 reglas (R1 a R7 en YAML, más el seguimiento de placas y el abuso de dispositivos) están
documentadas en [`docs/correlacion.md`](../correlacion.md), y la configuración vive en
`correlation-engine/src/main/resources/application.yml`.

**Por qué no aprendizaje automático:** no hay datos históricos etiquetados de incidentes reales, y
una caja negra es difícil de defender ante un juez o la ciudadanía. El diseño deja preparado el
camino: las validaciones de los operadores (`CONFIRMED` / `FALSE_POSITIVE`) generan exactamente el
conjunto etiquetado que haría falta para entrenar un modelo en una fase posterior.

## Consecuencias

**Positivas**
- **Explicabilidad total:** "Robo en curso en Zona 4. Puntaje 0,90 (umbral 0,70). Evidencia: botón
  de pánico ROBO (+0,40); exceso de velocidad 85 km/h (+0,30); placa XYZ*** a 87 km/h (+0,20)".
- Reglas ajustables por un analista sin programar; 15 pruebas unitarias usan **las mismas reglas del
  YAML** (tabla 2.4, falsos positivos, abuso, fuga, disparos, incendio, accidente, umbral adaptativo).
- **Validado en vivo:** 9 de 9 correlaciones esperadas detectadas en cada corrida, con latencias de
  28 a 477 ms.
- Escala de forma horizontal: estado en Redis y llave de zona en Kafka.

**Negativas (aceptadas)**
- Los pesos se fijaron por criterio experto y no se aprendieron de datos. Se mitiga con los umbrales
  adaptativos y la revisión periódica de la precisión (dashboard de falsos positivos).
- Puede haber falsos positivos por coincidencia, por ejemplo un pánico real junto a un exceso de
  velocidad no relacionado. En la simulación aparecen y los operadores los marcan, lo que alimenta el
  ajuste de umbrales.
- Redis es una dependencia crítica: si cae, el motor espera y reintenta hasta 2 min (ADR-006).

## Impacto en el negocio

- **Costo:** sin licencias ni clúster adicional (Flink). Usa el Redis ya presupuestado.
- **Riesgo:** cada alerta se puede justificar ante la dirección, el área legal o un juez. Los falsos
  positivos, que gastan patrullas y desgastan la confianza ciudadana, se miden y se reducen con el
  uso del sistema.
- **Tiempo:** una regla nueva, por ejemplo "aglomeración anómala", se agrega en minutos editando la
  configuración.
