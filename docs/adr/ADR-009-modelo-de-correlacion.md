# ADR-009: Modelo de correlación por evidencia ponderada con ventanas en Redis

| Campo | Valor |
|---|---|
| Estado | Aceptado (revisado el 2026-10-22 tras grabar el video y el 2026-10-25 tras la revisión final) |
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
| Cambiar reglas sin recompilar | No | **Sí** (YAML montado desde `infra/correlation`) | No | Sí (EPL/SQL) | Reentrenar |
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
    nuevo testigo que es ancla, como otro pánico (UPDATED). No se repite la alerta;
  - las coincidencias que alcanzan el umbral se prueban de mayor a menor puntaje hasta que una aporte
    algo nuevo (corrección hecha al grabar el video: un segundo robo con el incidente abierto salía
    recién a los 90 s);
  - una cámara que lee **otra** placa es otro vehículo y no corrobora a un ancla de placa (R6;
    también corregido al grabar el video).
- **Falsos positivos:**
  - un ancla que no se corrobora en 90 s produce `POSIBLE_FALSO_POSITIVO`, con severidad baja;
  - un dispositivo que presiona pánico más de 5 veces en 10 min se marca como sospechoso y se ignora
    durante 1 h;
  - un disparo aislado (posible pirotecnia) no basta: dos sensores acústicos solos suman 0,65
    (< 0,70), hace falta además un pánico (+0,30) o un reporte (+0,20);
  - mientras el sistema se pone al día tras una caída no se clasifica ningún falso positivo, porque la
    evidencia puede venir detrás en la cola (detección sin relojes, ADR-006).
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
`correlation-engine/src/main/resources/application.yml`. Para cambiarlas sin recompilar se copia la
sección `correlation:` a `infra/correlation/application.yml`, que los contenedores del motor leen como
configuración adicional, y se reinicia el motor; el archivo debe ser el mismo en todas las PCs que lo
corren. Al arrancar, el motor valida las reglas y no inicia si una es inválida.

**Por qué no aprendizaje automático:** no hay datos históricos etiquetados de incidentes reales, y
una caja negra es difícil de defender ante un juez o la ciudadanía. El diseño deja preparado el
camino: las validaciones de los operadores (`CONFIRMED` / `FALSE_POSITIVE`) generan exactamente el
conjunto etiquetado que haría falta para entrenar un modelo en una fase posterior.

## Consecuencias

**Positivas**
- **Explicabilidad total:** "Robo en curso en Zona 4. Puntaje 0,90 (umbral 0,70). Evidencia: botón
  de pánico ROBO (+0,40); exceso de velocidad 85 km/h (+0,30); placa XYZ*** a 87 km/h (+0,20)".
- Reglas ajustables por un analista sin programar; 17 pruebas del modelo usan **las mismas reglas del
  YAML** (tabla 2.4, falsos positivos, abuso, fuga, disparos, incendio, accidente, segundo robo con el
  incidente abierto, placa ajena, umbral adaptativo), y otras 11 prueban la resiliencia del motor.
- **Validado en vivo:** 9 de 9 correlaciones esperadas detectadas en cada corrida, con latencias de
  28 a 477 ms. Los escenarios corren aislados del ruido de fondo, así que el resultado no depende del
  azar (antes, con el tráfico de fondo encendido, la fila 2 de la tabla 2.4 fallaba en ≈ 15 % de las
  corridas).
- Escala de forma horizontal: estado en Redis y llave de zona en Kafka.

**Negativas (aceptadas)**
- Los pesos se fijaron por criterio experto y no se aprendieron de datos. Se mitiga con los umbrales
  adaptativos y la revisión periódica de la precisión (dashboard de falsos positivos).
- Puede haber falsos positivos por coincidencia, por ejemplo un pánico real junto a un exceso de
  velocidad no relacionado. En la simulación aparecen y los operadores los marcan, lo que alimenta el
  ajuste de umbrales.
- Redis es una dependencia crítica: si cae, el motor lo trata como falla transitoria y reintenta sin
  límite (1–30 s), nada va a la DLQ y, si la caída se prolonga, el monitor de SLO avisa al operador
  (rezago sostenido, ADR-008).

## Impacto en el negocio

- **Costo:** sin licencias ni clúster adicional (Flink). Usa el Redis ya presupuestado.
- **Riesgo:** cada alerta se puede justificar ante la dirección, el área legal o un juez. Los falsos
  positivos, que gastan patrullas y desgastan la confianza ciudadana, se miden y se reducen con el
  uso del sistema.
- **Tiempo:** cambiar un umbral o un peso toma minutos (editar la configuración y reiniciar el motor);
  una regla nueva con un tipo de evidencia ya existente, por ejemplo "aglomeración anómala", se agrega
  en una tarde con sus pruebas.
