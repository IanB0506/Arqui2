# Guía de estudio para la defensa

Esta guía resume lo que hay que **entender**, no solo lo que hay que mostrar. La evaluación es en
vivo y "no se evaluará únicamente qué se implementó, sino la calidad del razonamiento detrás de cada
elección" (sección 4).

---

## 1. El proyecto en dos minutos (para abrir la presentación)

> "Diseñamos Centinela Urbano para una municipalidad mediana de Guatemala, con 12 zonas y presupuesto
> limitado. Cinco tipos de sensores simulados (botones de pánico, cámaras de placas, sensores de
> velocidad, sensores acústicos y reportes ciudadanos) envían eventos a Kafka. Un servicio de ingesta
> los valida y **seudonimiza las placas** para que ningún dato personal quede en claro. Un motor de
> correlación combina eventos de fuentes distintas en la misma zona y ventana de tiempo y calcula un
> puntaje de evidencia. Por ejemplo: pánico más exceso de velocidad más placa a alta velocidad da
> 0,90, es decir, robo en curso crítico. Un pánico solo, sin nada más, se marca como posible falso
> positivo. Las alertas llegan a una consola en vivo y a Grafana en unos 50 milisegundos, muy por
> debajo del objetivo de 2 segundos. Elegimos on-premise porque los datos de placas no deben salir de
> la municipalidad y porque sale entre un 30 y un 40 % más barato que la nube a 3 años. Todo está en
> Java con Spring Boot, se puede repartir entre varias máquinas y validamos 9 de 9 correlaciones
> esperadas. Apagamos cada servidor a propósito: si cae el de servicios, la detección sigue en otra
> máquina y un vigilante lo anuncia en segundos; si cae el de datos, ningún evento crítico se pierde."

---

## 2. Conceptos que deben dominar

### Arquitectura orientada a eventos (EDA)
- Los componentes **no se llaman entre sí**: publican "algo pasó" (un evento) y quien esté interesado
  lo consume.
- **Ventaja:** desacoplamiento. El simulador no sabe que existe el motor de correlación; se puede
  agregar un consumidor nuevo, como Elasticsearch en el futuro, sin tocar nada.
- **Desventaja:** depurar es más difícil. Por eso se usa el `event_id` de punta a punta.

### Broker, topics, particiones y grupos de consumidores (Kafka)
- **Topic:** un "canal" con nombre (`sensor.panic.raw`).
- **Partición:** un topic se divide en partes ordenadas. **El orden solo se garantiza dentro de una
  partición.** Por eso la llave del mensaje normalizado es la **zona**: todos los eventos de la Zona 4
  van a la misma partición y llegan en orden al mismo consumidor.
- **Grupo de consumidores:** varias instancias con el mismo `group-id` se reparten las particiones.
  Con 12 particiones y 2 máquinas de 6 hilos, cada máquina toma 6. Si una cae, la otra toma las 12.
  **Así se reparte la carga entre máquinas.**
- **Offset:** la posición de lectura de cada grupo. Grupos distintos leen el mismo topic a su propio
  ritmo: `correlation` en tiempo real y `event-store` guardando en la base.
- **Retención:** Kafka no borra al consumir; guarda 24 a 72 h. Si un servicio cae, retoma donde se quedó.

### Correlación de eventos y CEP (procesamiento de eventos complejos)
- **Correlacionar:** inferir algo que ningún evento dice por sí solo, combinando varios.
- **Ventana de tiempo:** se buscan eventos relacionados dentro de un intervalo
  (`[ancla − 2 min, ancla + 5 min]`). Las ventanas viven en Redis como *sorted sets* cuyo puntaje es
  la hora del evento.
- **Hora del evento vs hora de proceso:** se correlaciona por la hora en que **ocurrió** el evento
  (`timestamp` del sensor), no por la hora en que llegó. Por eso el desorden no importa.
- **Evidencia ponderada:** cada evidencia suma un peso y el puntaje se compara con un umbral.
  **Es explicable:** se puede decir exactamente por qué se generó la alerta.

### Procesamiento en tiempo real vs por lotes
- **Tiempo real:** el motor de correlación procesa cada evento al llegar (milisegundos).
- **Por lotes:** el almacenamiento inserta cientos de eventos por transacción, y las tareas periódicas
  calculan estadísticas cada hora.
- El sistema usa los dos, **con consumidores separados**, para que lo lento no frene lo urgente.

### Garantías de entrega
- **Como máximo una vez:** se puede perder, nunca duplica.
- **Al menos una vez (el que usamos):** nunca se pierde lo aceptado, pero puede duplicar. Los
  duplicados se neutralizan con **idempotencia**:
  - deduplicación por `event_id` en Redis, con la "ficha" de la entrega (`topic:partición:offset`):
    el reintento del **mismo** mensaje no se descarta, otro envío del mismo evento sí;
  - `ON CONFLICT DO NOTHING` en la base;
  - *upsert* de alertas por revisión, con **ID determinista**: si el motor muere a mitad de una
    alerta y otra instancia la recalcula, sale con el mismo ID y no se duplica.
- **Idempotente:** hacer la operación dos veces da el mismo resultado que hacerla una vez.

### Escritura anticipada (*write-ahead*)
- Primero se escribe en disco y **después** se envía; al confirmar Kafka, se marca como hecho. Así un
  evento crítico nunca vive solo en la memoria de un proceso: si el proceso muere o se va la luz, al
  arrancar se relee el diario y se reenvía lo pendiente.
- Es la misma idea que usan las bases de datos (WAL) para no perder transacciones.

### Monitor del monitor (vigilante)
- Si se cae el servidor que muestra las alertas, nadie lo ve en la consola, porque la consola es lo
  que se cayó. Por eso el vigilante corre **en otra máquina** (nodo C): revisa cada 10 s al nodo B y a
  Kafka y dice qué cayó y dónde sigue funcionando la detección.

### DLQ (Dead Letter Queue, cola de mensajes muertos)
- Un mensaje que **nunca** se podrá procesar, como un JSON roto, no debe reintentarse para siempre ni
  bloquear a los demás ("mensaje veneno"). Va a `events.dlq` con su causa para revisarlo después.
- Se clasifica cada falla:
  - **permanente** (inválido): directo a la DLQ;
  - **transitoria** (Redis, la base o Kafka no responden): se reintenta **sin límite** con espera
    exponencial de 1 s a 30 s; el mensaje espera en Kafka, no va a la DLQ;
  - **desconocida**: 4 reintentos y luego a la DLQ.
- Una tarea cada 5 minutos **reprocesa** la DLQ: reinyecta lo que falló por causas pasajeras (hasta 3
  veces). Lo permanente solo a pedido de un administrador, tras corregir la causa.

### Seudonimización vs anonimización vs cifrado
- **Cifrado:** reversible con la llave.
- **Anonimización:** irreversible; ya no se puede saber de quién es el dato.
- **Seudonimización (la que usamos):** reemplaza el dato por un seudónimo estable. Con HMAC y llave
  secreta, la misma placa siempre da el mismo seudónimo, así que **se puede seguir un vehículo y
  comparar con la lista de robados sin conocer la placa**.
- **¿Por qué no SHA-256 simple?** Las placas tienen pocas combinaciones (≈ 10⁷): un atacante calcula
  el hash de todas en minutos y revierte el seudónimo. Con HMAC necesita la llave.
- **Generalización:** redondear las coordenadas del ciudadano a unos 110 m.

### Series de tiempo (TimescaleDB)
- **Hypertable:** una tabla que TimescaleDB divide automáticamente en partes por tiempo (*chunks*
  diarios). Consultar "los últimos 15 minutos" solo lee el chunk de hoy.
- **Compresión por columnas** de los chunks viejos: alrededor de un 90 % menos de espacio.
- **Retención:** borra los chunks de más de 90 días.
- **Agregado continuo:** una vista materializada que se actualiza sola (eventos por minuto).

### SLI, SLO y SLA
- **SLI (indicador):** lo que se mide, por ejemplo la latencia del evento a la alerta.
- **SLO (objetivo):** la meta interna, por ejemplo el 95 % de las alertas en menos de 2 s.
- **SLA (acuerdo):** un compromiso contractual con consecuencias, por ejemplo con un proveedor. No
  aplica aquí.
- **Lo que pide el enunciado: qué pasa cuando se incumple.** Nuestra respuesta:
  1. degradación controlada automática (se muestrean las lecturas de velocidad normales);
  2. alerta de sistema en la consola;
  3. registro del incumplimiento;
  4. procedimiento de escalado (levantar *workers*).

### Degradación controlada
- Ante sobrecarga, sacrificar lo menos importante (detalle estadístico de tránsito) para proteger lo
  crítico (pánicos, disparos). Es mejor que colapsar de forma pareja.
- Hay que saber **cuándo no** degradar: tras una caída, las alertas llegan tarde aunque no haya
  sobrecarga. Degradar ahí descartaría datos justo durante la puesta al día.

### Gobernanza de datos críticos
- Saber **qué datos hay, quién los ve, cuánto tiempo se guardan y cómo se audita**:
  - roles OPERATOR / ADMIN;
  - Grafana solo lee vistas sin seudónimos;
  - plazos de retención automáticos;
  - bitácora `audit_log`.

---

## 3. Mapa del código (dónde está cada cosa)

| Si preguntan por... | Archivo |
|---|---|
| Campos de cada sensor (sección 2.1) | `common/.../model/raw/*.java` |
| Seudonimización HMAC | `common/.../privacy/PlatePseudonymizer.java` |
| Zonas y coordenadas | `common/.../geo/ZoneCatalog.java` |
| Escenarios (tabla 2.4, etc.) | `sensor-simulator/.../scenario/ScenarioCatalog.java` |
| Diario *write-ahead* en disco | `common/.../spool/DiskSpool.java` |
| Qué se guarda si cae Kafka (crítico, telemetría útil, descarte) | `sensor-simulator/.../publish/SpoolPolicy.java` y `SpoolService.java` |
| Gateway de la app con spool (202 `EN_COLA`) | `ingestion-service/.../api/GatewaySpool.java` |
| Vigilante del nodo B | `sensor-simulator/.../watchdog/NodeWatchdog.java` (página `/vigilante.html`) |
| Validación, normalización y anomalías | `ingestion-service/.../pipeline/EventNormalizer.java` |
| Clases de falla, reintentos y DLQ | `common/.../kafka/FailureClassifier.java` y `ResilientErrorHandlers.java` |
| Deduplicación con ficha de entrega | `ingestion-service/.../pipeline/RedisDedupStore.java` |
| Reproceso de la DLQ | `alert-service/.../dlq/DlqReprocessor.java` |
| Lista de placas autorreparable | `alert-service/.../jobs/WatchlistSyncJob.java` |
| **Reglas de correlación** | `correlation-engine/src/main/resources/application.yml` |
| Cálculo del puntaje | `correlation-engine/.../engine/RuleEvaluator.java` |
| Incidentes, falsos positivos, fuga | `correlation-engine/.../engine/CorrelationEngine.java` |
| Ventanas en Redis | `correlation-engine/.../engine/RedisWindowStore.java` |
| Pruebas del modelo | `correlation-engine/src/test/.../CorrelationEngineTest.java` |
| Esquema de la base | `alert-service/src/main/resources/db/migration/*.sql` |
| Monitor de SLO, causa (sobrecarga o recuperación) y degradación | `alert-service/.../jobs/SloMonitorJob.java` |
| Retención de placas | `alert-service/.../jobs/PlateRetentionJob.java` |
| Ajuste adaptativo de umbrales | `alert-service/.../jobs/RuleQualityJob.java` |
| Consola del operador | `alert-service/src/main/resources/static/` |
| Dashboards | `infra/grafana/dashboards/*.json` |
| Despliegue por nodo | `deploy/nodo-a`, `deploy/nodo-b`, `deploy/nodo-c` |
| Operación desde Windows | `centinela.cmd` / `centinela.ps1` |

---

## 4. Preguntas probables y cómo responderlas

**Plataforma (ADR-001)**

1. *¿Por qué on-premise y no la nube, si la nube es elástica?*
   - El cliente es una municipalidad mediana: le conviene un gasto predecible y los datos de placas
     no deben salir de su infraestructura.
   - La detección debe funcionar aunque se caiga internet.
   - A 3 años sale ≈ US$34 500 frente a ≈ US$48 000–57 000 en la nube.
   - Aceptamos la falta de elasticidad: dimensionamos para un pico de ×20 (medido: p95 = 67 ms) y,
     si se supera, hay degradación controlada y escalado agregando *workers*.
2. *¿Cuándo recomendarían híbrido?* Si la municipalidad quisiera analítica regional o cruzar datos
   con otras municipalidades: se enviarían a la nube **solo agregados anónimos** (conteos por zona y
   hora), nunca placas.
3. *¿Cómo calcularon la latencia esperada?* Por tramos (LAN, ingesta, Redis, base de datos), unos
   50–150 ms en total, y luego la **medimos**: p50 de 39 a 78 ms.

**Broker (ADR-002)**

4. *¿Por qué Kafka y no RabbitMQ?*
   - Necesitamos que **tres consumidores distintos lean los mismos eventos** a su ritmo y poder
     reprocesar. En RabbitMQ el mensaje se borra al consumirse.
   - Necesitamos orden por zona (particiones con llave) y repartir la carga entre máquinas (grupos
     de consumidores).
5. *¿Qué pasa si se cae Kafka?*
   - Los eventos críticos (pánico, reportes, disparos) se escriben en disco **antes** de enviarse y
     se reenvían al volver. El gateway de la app los guarda en disco si Kafka no confirma en 3 s y
     responde 202 `EN_COLA`. Lo probamos: 0 pérdidas, también con un `kill -9` del simulador en plena
     caída.
   - La telemetría útil (placas, excesos, sonidos relevantes) se guarda con tope de 15 minutos; las
     lecturas normales se pierden, y eso es aceptable según la política del ADR-006.
6. *¿Por qué 12 particiones?* Es el máximo de consumidores en paralelo por grupo: 2 máquinas × 6 hilos.
7. *¿Por qué un topic por tipo de sensor?* Para aislar la carga (una avalancha de LPR no retrasa los
   pánicos) y para que cada esquema evolucione por separado.

**Persistencia (ADR-003)**

8. *¿Por qué TimescaleDB y no InfluxDB o MongoDB?*
   - Tenemos datos de series de tiempo (eventos) **y** relacionales (alertas, auditoría): TimescaleDB
     hace las dos cosas con SQL estándar, con una sola base que respaldar.
   - InfluxDB obligaría a mantener una segunda base para las alertas.
9. *¿Por qué no usaron Elasticsearch, si está en el stack sugerido?*
   - La búsqueda que necesitamos (zona, tipo, placa seudonimizada, rango de fechas) se resuelve con
     índices de TimescaleDB.
   - Elasticsearch duplicaría datos y consumiría unos 2 GB de RAM más.
   - Si se necesita en el futuro, se agrega como otro consumidor de Kafka sin tocar nada.

**Tareas periódicas (ADR-004)**

10. *¿Por qué no Airflow?*
    - Son 7 tareas cortas e independientes, sin dependencias entre sí (SLO, retención, expiración,
      calidad de reglas, lista de placas, su revisión cada 15 s y el reproceso de la DLQ).
    - Airflow agrega 4 a 5 contenedores y 2 a 4 GB de RAM, y obliga a escribir las tareas en Python.
    - Usamos Spring Scheduler + **ShedLock**, que garantiza que cada tarea corra una sola vez aunque
      haya varias instancias.
    - Revisaríamos la decisión si hubiera flujos de varios pasos o entrenamiento real de modelos.

**Contenedores (ADR-005)**

11. *¿Por qué no Kubernetes?*
    - Con 3 o 4 máquinas, su plano de control consume recursos y su red superpuesta añade riesgo el
      día de la demo.
    - El escalado que necesitamos (agregar consumidores) ya lo resuelve Kafka.
    - Migraríamos con más de unas 10 máquinas o con requisitos de alta disponibilidad automática; las
      imágenes ya son compatibles.

**Resiliencia (ADR-006)**

12. *¿Qué es la pérdida aceptable en su sistema?* Pánico, reportes y disparos: 0, también desde la
    app. Telemetría útil durante una caída del broker: se guarda la de los últimos 15 min. Lecturas
    normales durante la caída: se pierden. En modo degradado: se conserva el 10 % de las velocidades
    normales.
13. *¿Qué pasa con un mensaje mal formado?* Va directo a la DLQ como falla PERMANENTE, sin
    reintentos, y no bloquea a los demás. Lo demostramos inyectando mensajes con
    `kafka-console-producer` y viéndolos con `centinela.cmd dlq`.
14. *¿Y si un evento llega dos veces?* Se deduplica por `event_id` en Redis (distinguiendo un
    reintento del mismo mensaje de un duplicado real) y la base lo ignora por su llave primaria.
15. *¿Qué hacen los dos servidores y qué pasa si se cae cada uno?*
    - **A guarda el estado** (Kafka, la base, Redis); **B lo procesa** (ingesta, correlación,
      alertas, Grafana). B no guarda nada propio, por eso se puede reemplazar.
    - **Cae B:** solo hay retraso. Los workers del nodo C siguen detectando, Kafka guarda el resto,
      el vigilante lo anuncia en ~16 s y la consola avisa; al volver, recupera y destaca lo perdido.
    - **Cae A:** no se detecta nada nuevo mientras dure, pero no se pierde nada crítico: se guarda en
      disco en los nodos generadores y en el gateway. Al volver, un robo ocurrido durante la caída
      sale como ROBO_EN_CURSO (no como falso positivo) y la lista de placas se recarga sola.
    - A es el punto único de falla aceptado para el alcance académico; en producción se replica.
16. *¿Cómo se enteran de que se cayó el servidor de la consola?* Con el vigilante del nodo C (página
    `/vigilante.html` y `centinela.cmd estado`) y con el aviso "Sin conexión" de la propia consola.
17. *¿Por qué al volver de una caída no se activa el modo degradado si el SLO está en rojo?* Porque
    las alertas tardías son eventos que esperaron durante la caída. El monitor mira el atraso de
    Kafka: si es bajo o baja, es **recuperación** (se avisa, no se degrada); solo si es grande y no
    baja, es **sobrecarga**.

**Seguridad y privacidad (ADR-007)**

18. *¿Cómo protegen las placas?*
    - Se seudonimizan con HMAC-SHA256 en la ingesta; la placa en claro nunca se guarda.
    - El operador ve `XYZ***`.
    - El seudónimo se borra a los 30 días.
    - Grafana no ve seudónimos.
19. *¿Cómo identifican un vehículo para un juez, entonces?* Con un procedimiento formal: se aplica el
    HMAC a la placa que la autoridad investiga y se busca ese seudónimo. No es posible listar todas
    las placas, y esa limitación es intencional.
20. *¿Qué ley aplica?* El Decreto 57-2008 (Ley de Acceso a la Información Pública: datos sensibles y
    habeas data) para la municipalidad como sujeto obligado. Como referencia internacional, los
    principios del RGPD: minimización, limitación del plazo y privacidad desde el diseño.

**Observabilidad (ADR-008)**

21. *¿Cuál es su SLO y qué pasa si se incumple?*
    - El 95 % de los avisos HIGH/CRITICAL deben quedar persistidos en menos de 2 s desde el evento.
    - Si se incumple, primero mira la causa: si el atraso de Kafka es grande y no baja
      (**sobrecarga**), activa solo el modo degradado, emite una alerta de sistema y el supervisor
      sigue el procedimiento (levantar *workers*); si es una **recuperación** tras una caída, solo
      avisa.
    - Lo vimos ocurrir en la prueba de sobrecarga ×100 (sobrecarga) y en las caídas de nodos
      (recuperación).
22. *¿Cómo saben dónde está el cuello de botella?* Por el **lag de los consumidores** en Prometheus:
    en la sobrecarga el lag del motor de correlación subió a 5 762 mensajes. Optimizamos el motor y
    el lag quedó en 0.

**Correlación (ADR-009)**

23. *Expliquen la fila 1 de la tabla 2.4.* Pánico ROBO en la Zona 4 (+0,40), exceso de velocidad de
    85 km/h en la misma zona (+0,30) y placa XYZ123 a alta velocidad en una cámara de la misma zona
    (+0,20): 0,90, CRITICAL. Con el escenario `tabla-2-4` (llega primero la placa) sale directo en
    CRITICAL; con `robo-en-curso` (llega primero la velocidad) se ve escalar: 0,70 HIGH y luego 0,90
    CRITICAL. La placa queda sospechosa: si aparece en la Zona 8, se genera `VEHICULO_EN_FUGA`.
24. *¿Y la fila 2?* El pánico de la Zona 10 suma 0,40, menor que 0,70. Queda pendiente 90 s; como no
    llega evidencia, se clasifica como `POSIBLE_FALSO_POSITIVO`, con severidad baja, para que el
    operador lo verifique con una llamada.
25. *¿Por qué no usaron inteligencia artificial?*
    - No hay datos históricos etiquetados de incidentes reales, y una caja negra es difícil de
      defender ante un juez.
    - Nuestro modelo es explicable.
    - Las validaciones de los operadores ya generan los datos etiquetados para entrenar un modelo
      en el futuro.
26. *¿Cómo manejan los falsos positivos?*
    - Exigiendo corroboración y marcando dispositivos abusivos.
    - Pidiendo dos señales para los disparos (por los cohetes).
    - Con umbrales adaptativos: si en una zona hay muchos falsos positivos validados, se exige más
      evidencia (+0,10).
27. *¿Qué pasa si los eventos llegan desordenados?* Se correlaciona por la hora del evento y se
    evalúa tanto al llegar el ancla como al llegar la evidencia, así que el orden no importa.
28. *¿Cómo evitan alertas duplicadas?* Un incidente por tipo y zona durante 10 minutos: la nueva
    evidencia lo **actualiza** (UPDATED/ESCALATED) en lugar de crear otro.

**Stack (ADR-010)**

29. *¿Por qué microservicios y no un monolito?* Por el requisito de arquitectura distribuida y para
    repartir la carga entre máquinas: cada servicio escala por separado. Lo probamos con el motor en
    dos nodos.
30. *¿Por qué el simulador en Java y no en Python, como sugiere el enunciado?* El curso exige Java;
    además reutiliza los mismos contratos de eventos (módulo `common`) y permite escenarios
    reproducibles con una API para la demo.

**Generales**

31. *¿Qué harían distinto en producción?*
    - 3 brokers de Kafka (factor de réplica 3), una réplica de PostgreSQL y Redis con Sentinel;
    - TLS y SASL;
    - llave HMAC en un gestor de secretos;
    - integración con el directorio de usuarios municipal;
    - NTP obligatorio.
32. *¿Qué aprendieron de las pruebas?* Que probar los escenarios de falla descubre defectos que el
    camino feliz no muestra. Primero encontramos 4 (entre ellos el spool sin permisos de escritura);
    después, apagando de verdad cada servidor, otras 8 brechas entre lo que el ADR prometía y lo que
    el sistema hacía (por ejemplo, un robo durante la caída de A clasificado como falso positivo).
    Todas están corregidas y vueltas a probar (ver `docs/resultados.md` y ADR-006).
33. *¿Cómo se compara con casos reales?* Ver la sección 5.

---

## 5. Comparación con ciudades inteligentes (resumen para la defensa)

| Caso real | Qué hace | Relación con nuestro diseño |
|---|---|---|
| **Centro de Operações Rio** (Río de Janeiro, 2010) | Integra la información de unas 30 agencias municipales y estatales en un centro de control, creado tras las inundaciones de 2010 | Mismo concepto de "centro de monitoreo" que unifica fuentes. Nosotros, a escala municipal y on-premise. |
| **C5 de la Ciudad de México** | Más de 14 000 postes con cámaras y unos 10 000 botones de auxilio (cifras de 2019–2020); reportes de fallas de cámaras y botones (2025) | Valida el botón de pánico como sensor urbano. Las fallas y el uso indebido justifican nuestra detección de dispositivos abusivos o defectuosos. |
| **ShotSpotter en Chicago** (detección acústica de disparos) | El Inspector General (2021) encontró que solo el 9,1 % de las alertas con despacho policial produjo evidencia de un delito con arma | Justifica que un sensor acústico **solo** no baste: exigimos corroboración y medimos los falsos positivos. |
| **ANPR en Reino Unido** (lectores de placas) | Las lecturas en el servicio nacional se borran a los 12 meses salvo preservación legal; los sistemas locales a los 7 días | Referencia para limitar la retención; nosotros fijamos 30 días para los seudónimos y usamos seudonimización, que es más estricta. |

---

## 6. Reparto sugerido para la presentación

Se sugiere que cada integrante domine un bloque y que todos puedan explicar la tabla 2.4 (sección
10.3: participación equitativa):

| Integrante | Bloque | ADRs |
|---|---|---|
| 1 | Escenario de negocio, plataforma y costos | ADR-001, ADR-005 |
| 2 | Kafka, flujo de eventos, resiliencia y demo de la DLQ | ADR-002, ADR-006 |
| 3 | Motor de correlación, tabla 2.4 y falsos positivos | ADR-009 |
| 4 | Datos, privacidad y tareas periódicas | ADR-003, ADR-004, ADR-007 |
| 5 | SLO, Grafana y resultados de las pruebas | ADR-008, ADR-010 |

---

## 7. Lista de verificación antes del 31 de octubre de 2026

- [ ] Completar en la portada del informe los nombres, carnés y número de grupo.
- [ ] Abrir el informe en Word y actualizar el índice: clic derecho → *Actualizar campos*.
- [ ] Revisar el formato: carta, Arial 12, interlineado 1.5, márgenes de 2.54 cm, sangría y numeración.
- [ ] Construir las imágenes en casa y llevarlas al laboratorio (`centinela.cmd exportar-imagenes`
  y, allá, `centinela.cmd importar-imagenes`).
- [ ] Ensayar el guion de la demo (`docs/manual-operacion.md`, sección 4) al menos una vez en las
  máquinas reales, incluida la caída de B y de A.
- [ ] Cambiar contraseñas y la llave HMAC en `.env`.
- [ ] Sincronizar la hora de todas las máquinas (Configuración → Hora → *Sincronizar ahora*);
  `centinela.cmd estado` avisa si hay desfase.
- [ ] Correr `centinela.cmd puertos` y `centinela.cmd firewall` en cada PC del laboratorio.
- [ ] Tener listo el plan B: todo en una sola laptop (`centinela.cmd configurar` → una sola PC).
