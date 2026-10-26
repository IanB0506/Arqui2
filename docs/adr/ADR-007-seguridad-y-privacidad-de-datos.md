# ADR-007: Seguridad y privacidad de datos personales

| Campo | Valor |
|---|---|
| Estado | Aceptado (revisado el 2026-10-25 tras la revisión final) |
| Fecha | 2026-10-09 |
| Decisores | Equipo de arquitectura del proyecto (con revisión del área legal) |
| Relacionados | ADR-001, ADR-003, ADR-004 |

## Contexto

El sistema procesa datos que **identifican o permiten rastrear a personas**:

- **Placas vehiculares:** junto con la ubicación y la hora de cada cámara permiten reconstruir los
  desplazamientos de una persona.
- **Ubicación de ciudadanos** que presionan el botón de pánico o envían reportes desde la app; puede
  revelar su domicilio.
- **Identificadores de usuarios y dispositivos** de la app.
- **Texto libre** en los reportes ciudadanos.

El enunciado exige definir cómo se protegen, anonimizan y retienen, **aunque los datos sean
sintéticos**.

En Guatemala, el Decreto 57-2008, Ley de Acceso a la Información Pública (Congreso de la República de
Guatemala, 2008), protege los datos personales sensibles en poder de entidades públicas y regula el
habeas data. Una municipalidad es sujeto obligado de esa ley. Como referencia internacional se adoptan
los principios del Reglamento General de Protección de Datos europeo (Parlamento Europeo y Consejo de
la Unión Europea, 2016): minimización, limitación de la finalidad, limitación del plazo de
conservación y privacidad desde el diseño. También se toma como referencia comparada la práctica
policial británica para lectores de placas (ANPR), que elimina las lecturas a los 12 meses salvo que
un proceso legal exija conservarlas (Home Office, s. f.).

## Opciones consideradas

| Técnica para placas | Ventaja | Desventaja | Decisión |
|---|---|---|---|
| Guardar la placa en claro, con control de acceso | Investigación sencilla | Una fuga expone los movimientos de miles de personas | Descartada |
| Cifrado reversible (AES) | Recuperable por orden judicial | Requiere gestión de llaves; sigue siendo dato personal pleno | Descartada para el almacenamiento general |
| Hash simple (SHA-256) | Irreversible a simple vista | **Reversible por fuerza bruta**: las placas guatemaltecas tienen pocas combinaciones (≈ 10⁷) | Descartada |
| **Seudonimización HMAC-SHA256 con llave secreta + placa enmascarada** | Sin la llave es irreversible; la misma placa da el mismo seudónimo, lo que permite correlacionar y seguir vehículos | Si la llave se filtra, hay que rotarla | **Elegida** |

| Ubicación del ciudadano | Ventaja | Desventaja | Decisión |
|---|---|---|---|
| Coordenadas exactas | Despacho al punto preciso | Puede señalar la vivienda de quien pidió ayuda | Descartada |
| Solo la zona | Máxima privacidad | Una zona mide kilómetros: no sirve para despachar | Descartada |
| Celda *geohash* de 6 caracteres (≈ 1,2 × 0,6 km) | Estándar | Demasiado gruesa para una patrulla | Descartada |
| **Redondeo a 3 decimales (≈ 110 m)** | Alcanza para despachar sin señalar una casa | Pierde precisión fina | **Elegida** |

| Plazo de conservación del seudónimo de placa | Ventaja | Desventaja | Decisión |
|---|---|---|---|
| 12 meses, como el servicio ANPR británico | Más tiempo para investigaciones | Mucho más dato personal expuesto ante una fuga | Descartado |
| 90 días, como los eventos | Una sola política | El seudónimo permite reconstruir rutas durante tres meses | Descartado |
| **30 días** (las alertas confirmadas se conservan) | Cubre la investigación inmediata y reduce la exposición | Una investigación tardía debe pedir los registros a la cámara de origen | **Elegido** |

| Control de acceso | Decisión |
|---|---|
| API y consola sin autenticación | Descartada: cualquiera en la red vería alertas y podría validarlas |
| Una sola cuenta compartida | Descartada: no permite auditar quién validó qué |
| **Autenticación con roles (OPERATOR, ADMIN)** y bitácora de auditoría | **Elegida** (HTTP Basic en la simulación; en producción, OIDC/LDAP municipal) |

## Decisión

1. **Seudonimización en la puerta de entrada.** El `ingestion-service` reemplaza la placa por
   `HMAC-SHA256(llave, placa)` (128 bits) y una versión enmascarada para el operador (`XYZ***`). **La
   placa en claro no se escribe nunca** en Kafka normalizado, Redis, la base de datos ni los logs.
   - La lista de vehículos robados también se guarda seudonimizada: el administrador ingresa la placa,
     se seudonimiza al instante y se compara por seudónimo.
   - Los identificadores de ciudadanos y de dispositivos de la app también se seudonimizan.
   - Las placas que un ciudadano escriba en el texto libre de un reporte ("placa P-512KLM") también se
     enmascaran al entrar (`P51***`), y el simulador muestra las placas enmascaradas en sus registros.
2. **Generalización de ubicación:** las coordenadas que provienen de ciudadanos se redondean a 3
   decimales (≈ 110 m). Alcanza para despachar una patrulla y evita señalar una vivienda exacta.
3. **Minimización:**
   - del contexto del usuario de la app solo se conservan la plataforma y la versión, no el id;
   - el mensaje libre se recorta a 200 caracteres, sin placas en claro.
4. **Retención limitada:**

| Dato | Plazo | Cómo se aplica |
|---|---|---|
| Eventos crudos en Kafka | 24 h | Retención del topic |
| Eventos normalizados en Kafka | 72 h | Retención del topic |
| Seudónimo de placa en eventos y alertas no confirmadas | **30 días** | Tarea diaria `PlateRetentionJob`, auditada |
| Eventos (sin seudónimo) | 90 días | Política de retención de TimescaleDB |
| Alertas confirmadas | Se conservan (posible proceso legal) | Se excluyen de la purga |
| Estado de correlación en Redis | Minutos a 1 h | Expiración (TTL) de cada llave |
| Cola de mensajes muertos (`events.dlq`) | 7 días | Retención del topic. Guarda el mensaje original (puede traer una placa en claro): solo se consulta con rol ADMIN y la API muestra la causa, no el contenido |
| Alertas en Kafka (`alerts`) | 7 días | Retención del topic (solo placa enmascarada y seudónimo) |
| Spool en disco de la PC generadora | Críticos hasta que Kafka los confirma; telemetría 15 min | Se compacta al confirmarse; la telemetría vence sola |
| Respaldos de la base | ≈ 48 h (8 respaldos, uno cada 6 h) | Rotación automática del contenedor de respaldos |

5. **Mínimo privilegio:**
   - Grafana usa el usuario `grafana_reader`, que solo lee vistas **sin seudónimos**
     (`v_events`, `v_alerts`). En `v_events` el id de un celular, que es un trozo de su seudónimo, se
     muestra solo como `APP` (migración V5), y la descripción de una alerta de dispositivo sospechoso
     no lo menciona;
   - los operadores ven alertas y validan, pero no modifican la lista de placas;
   - solo ADMIN gestiona la lista y ejecuta tareas administrativas.
6. **Auditoría:** cada validación de alerta, cambio en la lista de placas, purga de datos y reinicio
   del estado de correlación queda en `audit_log` con quién, qué y cuándo.
7. **Secretos fuera del código:** la llave HMAC y las contraseñas van en variables de entorno (`.env`,
   excluido de Git). Si se usa la llave de desarrollo, el servicio emite una advertencia al arrancar.
8. **Segmentación de red** (esquema de infraestructura):

| Segmento | Equipos | Puede alcanzar |
|---|---|---|
| VLAN 10, sensores/borde | Nodos C/D, pasarelas | Nodo A: Kafka 9092 y Redis 6379. Nodo B: 8081–8083 (el vigilante revisa su salud y la validación consulta alertas) |
| VLAN 20, servicios | Nodo B | Nodo A: 9092, 5432 y 6379. Nodos C/D: 8080–8082 (Prometheus mide los generadores y *workers*) |
| VLAN 30, datos | Nodo A | No inicia conexiones |
| VLAN 40, operadores | Consolas del centro de monitoreo | Nodo B: 8083 y 3000. Nodo C: 8080 (vigilante, indispensable si cae B) |
| DMZ (producción) | Proxy inverso con TLS | Recibe la app ciudadana desde internet y solo la reenvía al gateway del nodo B (8081) |

Contenedores sin privilegios (usuario `centinela`) y Redis con contraseña.

**Pendiente para producción**, aceptado en la simulación: TLS en Kafka, PostgreSQL y HTTP; SASL/SCRAM
con ACL por servicio en Kafka; y la llave HMAC en un gestor de secretos con rotación anual.

## Consecuencias

**Positivas**
- Una fuga de la base de datos **no revela placas ni identidades**, solo seudónimos inútiles sin la llave.
- El sistema sigue correlacionando, siguiendo vehículos en fuga y comparando con la lista de robados
  sin conocer ninguna placa.
- Los plazos de conservación se cumplen de forma automática y verificable.
- Grafana, el componente más expuesto visualmente, no tiene acceso a seudónimos (verificado en la
  revisión final: se corrigió el id de los celulares en `v_events` y en la descripción de las alertas).

**Negativas (aceptadas)**
- Para identificar una placa ante un juez hace falta un procedimiento formal: aplicar el HMAC a la
  placa sospechosa para buscarla, o consultar los registros de la cámara de origen. La búsqueda
  inversa no es posible por diseño.
- Rotar la llave HMAC rompe la continuidad de seudónimos entre periodos; se planifica anual.
- La generalización a 110 m reduce algo la precisión del despacho.

## Impacto en el negocio

- **Costo:** US$0 en licencias; son decisiones de diseño implementadas en el software. En producción,
  los certificados TLS de una autoridad interna no tienen costo.
- **Riesgo:** reduce de forma drástica el riesgo legal y reputacional de una fuga de datos. Es el
  argumento central ante el área legal, que la municipalidad puede presentar a la ciudadanía como
  "vigilancia con privacidad por diseño".
- **Tiempo:** ya implementado. El paso a producción requiere unos 2 a 3 días para certificados TLS y
  la integración con el directorio de usuarios municipal.
