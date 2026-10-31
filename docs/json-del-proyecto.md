# JSON del proyecto Centinela Urbano

Todos los ejemplos de este documento son **reales**: se capturaron del sistema funcionando el 31 de
octubre de 2026, leyendo los topics de Kafka y llamando a las APIs mientras corrían los escenarios
`tabla-2-4`, `robo-en-curso`, `disparos`, `accidente-vial`, `vehiculo-robado` y `emergencia-medica`,
más pánicos y reportes enviados por la puerta HTTP de la app. La prueba se hizo con los nodos A, B y C
en un solo equipo; por eso aparece la IP de pruebas `192.0.2.2`. En el laboratorio aparecerán las IP
de cada PC.

Convenciones:

- Las fechas van en formato ISO-8601 y en UTC (`2026-10-31T18:19:35Z`). Guatemala es UTC−6.
- Los campos sin valor aparecen como `null`: el contrato no los omite.
- Las listas largas se recortan y se indica cuántos elementos tenían. Ningún otro dato se modificó.
- Los contratos están definidos en `common/src/main/java/gt/umg/centinela/common/model/`.

## Contenido

1. [Mensajes crudos de los sensores (Kafka)](#1-mensajes-crudos-de-los-sensores-kafka)
2. [Eventos normalizados (Kafka)](#2-eventos-normalizados-kafka)
3. [Alertas (Kafka)](#3-alertas-kafka)
4. [Cola de mensajes muertos, DLQ (Kafka)](#4-cola-de-mensajes-muertos-dlq-kafka)
5. [Gateway de la app (HTTP, nodo B)](#5-gateway-de-la-app-http-nodo-b)
6. [API de la consola: alert-service (HTTP, nodo B)](#6-api-de-la-consola-alert-service-http-nodo-b)
7. [API del simulador (HTTP, nodos C/D)](#7-api-del-simulador-http-nodos-cd)
8. [Archivos de configuración en JSON](#8-archivos-de-configuración-en-json)

## Mapa rápido

| Nivel | Dónde viaja | Lo produce | Lo consume | Nombres de los campos |
|---|---|---|---|---|
| Crudo | 5 topics `sensor.*.raw` | Simulador (C/D) y gateway de la app (B) | Ingesta (grupo `ingestion`) | Español, `snake_case` (enunciado, sección 2.1) |
| Normalizado | `events.normalized` | Ingesta (B y *workers*) | Correlación (`correlation`) y alert-service (`event-store`) | Inglés, `camelCase` (modelo interno) |
| Alerta | `alerts` | Correlación (B y *workers*) | Alert-service (`alert-store`) | Inglés, `camelCase` |
| Mensaje muerto | `events.dlq` | Ingesta, correlación o alert-service | Alert-service (consulta y reproceso) | El mensaje original intacto + cabeceras |
| APIs HTTP | Gateway (8081), consola (8083), simulador (8080) | Cada servicio | Navegador, app, `centinela.cmd` | Según el servicio |

**Un mismo evento a través de todos los niveles** (trazabilidad por `event_id`):

| Paso | Sección | `event_id` / `alertId` |
|---|---|---|
| La app envía un pánico ROBO en la Zona 9 | [5.1](#51-pánico-enviado-por-la-app-petición) | `edf5c3f8-0bae-4d44-a826-61b43c73d95a` |
| El gateway lo escribe en Kafka (crudo) | [1.1](#11-botón-de-pánico-desde-la-app-enviado-por-el-gateway) | `edf5c3f8-0bae-4d44-a826-61b43c73d95a` |
| La ingesta lo normaliza y seudonimiza | [2.1](#21-pánico-de-la-app-normalizado) | `edf5c3f8-0bae-4d44-a826-61b43c73d95a` |
| Nadie lo corrobora en 90 s: posible falso positivo | [3.4](#34-posible-falso-positivo-pánico-de-la-app-sin-corroborar) | alerta `772b288d-9895-3432-92f6-b773cf2a217e` |
| El operador lo valida en la consola | [6.5](#65-alerta-después-de-validarla) | alerta `772b288d-9895-3432-92f6-b773cf2a217e` |

## 1. Mensajes crudos de los sensores (Kafka)

Son el **contrato de entrada**: los campos del enunciado (sección 2.1), en español. Cada mensaje viaja con
una llave (el id del sensor o del dispositivo) y una cabecera `centinela-first-attempt` con la hora del
primer intento de envío.

### 1.1 Botón de pánico desde la app (enviado por el gateway)

**Topic:** `sensor.panic.raw` · **Lo produce:** gateway de la app (B) · **Llave:** `APP-58213` · **Partición:** 3 · **Cabecera:** `centinela-first-attempt: 1793470775639` (2026-10-31 18:19:35.639 UTC)

```json
{
  "event_id": "edf5c3f8-0bae-4d44-a826-61b43c73d95a",
  "timestamp": "2026-10-31T18:19:35Z",
  "zona_geografica": {
    "nombre": "Zona 9",
    "latitud": 14.604,
    "longitud": -90.516
  },
  "tipo_alerta": "ROBO",
  "identificador_dispositivo": "APP-58213",
  "user_context": {
    "usuario_id": "CIU-58213",
    "plataforma": "ANDROID",
    "version_app": "2.3.1"
  }
}
```

### 1.2 Botón de pánico de la app generado por el simulador

**Topic:** `sensor.panic.raw` · **Lo produce:** simulador (C/D), directo a Kafka · **Llave:** `APP-19208` · **Partición:** 0 · **Cabecera:** `centinela-first-attempt: 1793470844497` (2026-10-31 18:20:44.497 UTC)

```json
{
  "event_id": "6a6056de-2aed-4e68-bf74-0740bfb8cd3a",
  "timestamp": "2026-10-31T18:20:44.497479534Z",
  "zona_geografica": {
    "nombre": "Zona 5",
    "latitud": 14.625271694010944,
    "longitud": -90.50093065017501
  },
  "tipo_alerta": "ROBO",
  "identificador_dispositivo": "APP-19208",
  "user_context": {
    "usuario_id": "CIU-19208",
    "plataforma": "IOS",
    "version_app": "2.3.1"
  }
}
```

### 1.3 Botón de pánico físico (poste)

**Topic:** `sensor.panic.raw` · **Lo produce:** simulador (C/D) · **Llave:** `BTN-Z02-03` · **Partición:** 11 · **Cabecera:** `centinela-first-attempt: 1793470784574` (2026-10-31 18:19:44.574 UTC)

```json
{
  "event_id": "0e40ffee-2f99-4dfb-bcb2-e96e53dedb1e",
  "timestamp": "2026-10-31T18:19:44.574175436Z",
  "zona_geografica": {
    "nombre": "Zona 2",
    "latitud": 14.654381299418992,
    "longitud": -90.50769941171012
  },
  "tipo_alerta": "ASALTO",
  "identificador_dispositivo": "BTN-Z02-03",
  "user_context": null
}
```
_Un poste físico no trae `user_context`: va en `null`._

### 1.4 Lectura de una cámara LPR (placa en claro)

**Topic:** `sensor.lpr.raw` · **Lo produce:** simulador (C/D) · **Llave:** `LPR-Z04-01` · **Partición:** 4 · **Cabecera:** `centinela-first-attempt: 1793470783718` (2026-10-31 18:19:43.718 UTC)

```json
{
  "event_id": "a5c54d46-a5cf-4fc8-8f49-e8b7add7a547",
  "timestamp": "2026-10-31T18:19:43.718360079Z",
  "placa_vehicular": "XYZ123",
  "ubicacion_sensor": {
    "sensor_id": "LPR-Z04-01",
    "zona": "Zona 4",
    "latitud": 14.622100000000001,
    "longitud": -90.5191
  },
  "velocidad_estimada": 84.0,
  "modelo_vehiculo": "Hyundai Tucson",
  "color_vehiculo": "Gris"
}
```
_Es la placa XYZ123 de la fila 1 de la tabla 2.4. Esta es la única vez que la placa viaja en claro: la ingesta la seudonimiza (ver 2.3)._

### 1.5 Lectura de un sensor de velocidad

**Topic:** `sensor.speed.raw` · **Lo produce:** simulador (C/D) · **Llave:** `VEL-Z04-02` · **Partición:** 4 · **Cabecera:** `centinela-first-attempt: 1793470785715` (2026-10-31 18:19:45.715 UTC)

```json
{
  "event_id": "43c13d3f-fa6b-4029-8302-d444d562ad8d",
  "timestamp": "2026-10-31T18:19:45.715620074Z",
  "sensor_id": "VEL-Z04-02",
  "zona": "Zona 4",
  "velocidad_detectada": 85.0,
  "direccion": "ESTE",
  "contravia": false,
  "limite_velocidad": 60
}
```
_Los 85 km/h de la fila 1 de la tabla 2.4._

### 1.6 Detección de un sensor acústico (disparo)

**Topic:** `sensor.acoustic.raw` · **Lo produce:** simulador (C/D) · **Llave:** `ACU-Z02-01` · **Partición:** 7 · **Cabecera:** `centinela-first-attempt: 1793471193988` (2026-10-31 18:26:33.988 UTC)

```json
{
  "event_id": "e07463e2-8e0e-496c-bfca-dd1333a65b04",
  "timestamp": "2026-10-31T18:26:33.988018725Z",
  "sensor_id": "ACU-Z02-01",
  "zona": "Zona 2",
  "tipo_sonido_detectado": "DISPARO",
  "nivel_decibeles": 128.0,
  "probabilidad_evento_critico": 0.93
}
```

### 1.7 Reporte ciudadano con texto libre (dato no estructurado)

**Topic:** `sensor.citizen.raw` · **Lo produce:** gateway de la app (B) · **Llave:** `CIU-77120` · **Partición:** 4 · **Cabecera:** `centinela-first-attempt: 1793470775732` (2026-10-31 18:19:35.732 UTC)

```json
{
  "event_id": "a8cbf043-aec1-4c7e-b00a-e2fae12e2f2b",
  "timestamp": "2026-10-31T18:19:35Z",
  "tipo_evento": "ACCIDENTE",
  "mensaje_descriptivo": "Choque entre dos carros frente al mercado, el de placa P512KLM se dio a la fuga",
  "ubicacion_aproximada": {
    "nombre": null,
    "latitud": 14.6105,
    "longitud": -90.529
  },
  "origen": "APP",
  "reportante_id": "CIU-77120"
}
```
_El texto trae una placa en claro (P512KLM); la ingesta la enmascara (ver 2.6)._

### 1.8 Pánico reenviado desde el spool tras una caída de Kafka

**Topic:** `sensor.panic.raw` · **Lo produce:** gateway de la app (B), desde su `gateway.wal` · **Llave:** `APP-30417` · **Partición:** 9 · **Cabeceras:** `centinela-first-attempt: 1793471455507` (2026-10-31 18:30:55.507 UTC), `centinela-spooled: true`

```json
{
  "event_id": "0ced83f1-9fee-419c-82df-e48c5dbd3833",
  "timestamp": "2026-10-31T18:30:55Z",
  "zona_geografica": {
    "nombre": "Zona 8",
    "latitud": 14.6301,
    "longitud": -90.5389
  },
  "tipo_alerta": "ROBO",
  "identificador_dispositivo": "APP-30417",
  "user_context": {
    "usuario_id": "CIU-30417",
    "plataforma": "IOS",
    "version_app": "2.3.1"
  }
}
```
_Se envió con Kafka apagado: el gateway respondió EN_COLA (ver 5.3) y lo reenvió al volver Kafka. El cuerpo es idéntico; lo distinguen las cabeceras._

**Cabeceras de Kafka que agregan los productores** (viajan fuera del JSON para no alterar el contrato):

| Cabecera | Valor | Para qué |
|---|---|---|
| `centinela-first-attempt` | Hora del primer intento, en milisegundos desde 1970 | La ingesta mide la antigüedad del evento desde que entró al sistema, no desde que se reenvió |
| `centinela-spooled` | `true` | El mensaje salió de un spool en disco: llegó tarde por una caída |

## 2. Eventos normalizados (Kafka)

La ingesta convierte los cinco contratos en **un solo formato** (`NormalizedEvent`). Conserva el `event_id`,
resuelve la zona, calcula si el evento es anómalo y aplica la privacidad del ADR-007. La **llave es la
zona** y el particionador propio envía la zona N a la partición N − 1: en todos los ejemplos, la partición
es la zona menos uno.

### 2.1 Pánico de la app normalizado

**Topic:** `events.normalized` · **Lo produce:** ingesta (B o *worker*) · **Llave (zona):** `9` · **Partición:** 8

```json
{
  "eventId": "edf5c3f8-0bae-4d44-a826-61b43c73d95a",
  "sensorType": "PANIC",
  "sensorId": "APP-ddfbac30c201",
  "zoneId": 9,
  "zoneName": "Zona 9",
  "latitude": 14.604,
  "longitude": -90.516,
  "eventTime": "2026-10-31T18:19:35Z",
  "ingestedAt": "2026-10-31T18:19:35.723937917Z",
  "subtype": "ROBO",
  "value": null,
  "confidence": 1.0,
  "anomalous": true,
  "plateHash": null,
  "plateMasked": null,
  "sourceHash": "8082928465b98d8b1692b4250c36d2f3",
  "origin": "APP",
  "attributes": {
    "plataforma": "ANDROID",
    "version_app": "2.3.1"
  }
}
```
_Viene de 1.1. El dispositivo pasa a ser un seudónimo (`APP-` + 12 caracteres), el usuario se convierte en `sourceHash` y la ubicación se redondea a ≈ 110 m._

### 2.2 Pánico físico normalizado

**Topic:** `events.normalized` · **Lo produce:** ingesta (B o *worker*) · **Llave (zona):** `2` · **Partición:** 1

```json
{
  "eventId": "0e40ffee-2f99-4dfb-bcb2-e96e53dedb1e",
  "sensorType": "PANIC",
  "sensorId": "BTN-Z02-03",
  "zoneId": 2,
  "zoneName": "Zona 2",
  "latitude": 14.654381299418992,
  "longitude": -90.50769941171012,
  "eventTime": "2026-10-31T18:19:44.574175436Z",
  "ingestedAt": "2026-10-31T18:19:44.589024591Z",
  "subtype": "ASALTO",
  "value": null,
  "confidence": 1.0,
  "anomalous": true,
  "plateHash": null,
  "plateMasked": null,
  "sourceHash": "c4af0aaa91d7c551305fae2db2d39801",
  "origin": "PUNTO_FISICO",
  "attributes": {}
}
```
_Viene de 1.3. Un poste conserva su id y su ubicación exacta; `origin` es `PUNTO_FISICO`._

### 2.3 Lectura LPR normalizada (placa seudonimizada y enmascarada)

**Topic:** `events.normalized` · **Lo produce:** ingesta (B o *worker*) · **Llave (zona):** `4` · **Partición:** 3

```json
{
  "eventId": "a5c54d46-a5cf-4fc8-8f49-e8b7add7a547",
  "sensorType": "LPR",
  "sensorId": "LPR-Z04-01",
  "zoneId": 4,
  "zoneName": "Zona 4",
  "latitude": 14.622100000000001,
  "longitude": -90.5191,
  "eventTime": "2026-10-31T18:19:43.718360079Z",
  "ingestedAt": "2026-10-31T18:19:43.728990679Z",
  "subtype": "EXCESO_VELOCIDAD",
  "value": 84.0,
  "confidence": 1.0,
  "anomalous": true,
  "plateHash": "a8376828ab99020a5483a6face13db17",
  "plateMasked": "XYZ***",
  "sourceHash": null,
  "origin": "SENSOR",
  "attributes": {
    "modelo": "Hyundai Tucson",
    "color": "Gris"
  }
}
```
_Viene de 1.4. `plateHash` es el seudónimo HMAC (sirve para comparar con la lista de placas robadas) y `plateMasked` lo que ve el operador. 84 km/h supera 60 × 1,3 = 78, así que el subtipo es `EXCESO_VELOCIDAD`._

### 2.4 Lectura de velocidad normalizada

**Topic:** `events.normalized` · **Lo produce:** ingesta (B o *worker*) · **Llave (zona):** `4` · **Partición:** 3

```json
{
  "eventId": "43c13d3f-fa6b-4029-8302-d444d562ad8d",
  "sensorType": "SPEED",
  "sensorId": "VEL-Z04-02",
  "zoneId": 4,
  "zoneName": "Zona 4",
  "latitude": 14.624,
  "longitude": -90.5165,
  "eventTime": "2026-10-31T18:19:45.715620074Z",
  "ingestedAt": "2026-10-31T18:19:45.724086380Z",
  "subtype": "EXCESO_VELOCIDAD",
  "value": 85.0,
  "confidence": 1.0,
  "anomalous": true,
  "plateHash": null,
  "plateMasked": null,
  "sourceHash": null,
  "origin": "SENSOR",
  "attributes": {
    "direccion": "ESTE",
    "limite": "60"
  }
}
```
_Viene de 1.5. Como el sensor no envía coordenadas, se usa el centro de la zona._

### 2.5 Detección acústica normalizada

**Topic:** `events.normalized` · **Lo produce:** ingesta (B o *worker*) · **Llave (zona):** `2` · **Partición:** 1

```json
{
  "eventId": "e07463e2-8e0e-496c-bfca-dd1333a65b04",
  "sensorType": "ACOUSTIC",
  "sensorId": "ACU-Z02-01",
  "zoneId": 2,
  "zoneName": "Zona 2",
  "latitude": 14.6595,
  "longitude": -90.512,
  "eventTime": "2026-10-31T18:26:33.988018725Z",
  "ingestedAt": "2026-10-31T18:26:34.015711913Z",
  "subtype": "DISPARO",
  "value": 128.0,
  "confidence": 0.93,
  "anomalous": true,
  "plateHash": null,
  "plateMasked": null,
  "sourceHash": null,
  "origin": "SENSOR",
  "attributes": {}
}
```
_Viene de 1.6. `value` son los decibeles y `confidence` la probabilidad del sensor._

### 2.6 Reporte ciudadano normalizado (placa enmascarada en el texto)

**Topic:** `events.normalized` · **Lo produce:** ingesta (B o *worker*) · **Llave (zona):** `9` · **Partición:** 8

```json
{
  "eventId": "a8cbf043-aec1-4c7e-b00a-e2fae12e2f2b",
  "sensorType": "CITIZEN_REPORT",
  "sensorId": "REPORTE-APP",
  "zoneId": 9,
  "zoneName": "Zona 9",
  "latitude": 14.611,
  "longitude": -90.529,
  "eventTime": "2026-10-31T18:19:35Z",
  "ingestedAt": "2026-10-31T18:19:35.782439672Z",
  "subtype": "ACCIDENTE",
  "value": null,
  "confidence": 1.0,
  "anomalous": true,
  "plateHash": null,
  "plateMasked": null,
  "sourceHash": "499eda1ef442344f1e10db7126f0d576",
  "origin": "APP",
  "attributes": {
    "mensaje": "Choque entre dos carros frente al mercado, el de placa P51*** se dio a la fuga"
  }
}
```
_Viene de 1.7. El texto libre se conserva en `attributes.mensaje` con la placa enmascarada (P51***) y el reportante seudonimizado._

### 2.7 Evento normalizado que llegó desde un spool

**Topic:** `events.normalized` · **Lo produce:** ingesta (B o *worker*) · **Llave (zona):** `8` · **Partición:** 7

```json
{
  "eventId": "0ced83f1-9fee-419c-82df-e48c5dbd3833",
  "sensorType": "PANIC",
  "sensorId": "APP-b8b50293ef62",
  "zoneId": 8,
  "zoneName": "Zona 8",
  "latitude": 14.63,
  "longitude": -90.539,
  "eventTime": "2026-10-31T18:30:55Z",
  "ingestedAt": "2026-10-31T18:31:15.047222986Z",
  "subtype": "ROBO",
  "value": null,
  "confidence": 1.0,
  "anomalous": true,
  "plateHash": null,
  "plateMasked": null,
  "sourceHash": "fdaccd64b90609dd5674883f2c5e622c",
  "origin": "APP",
  "attributes": {
    "plataforma": "IOS",
    "version_app": "2.3.1",
    "reenviado_spool": "true",
    "retraso_origen_s": "20"
  }
}
```
_Viene de 1.8. La ingesta agrega `reenviado_spool` y `retraso_origen_s` (llegó 20 s tarde) para que el sistema sepa que esperó durante una caída._

## 3. Alertas (Kafka)

Las publica el motor de correlación. Un incidente puede publicarse varias veces con el **mismo `alertId`**
y una `revision` mayor; el alert-service hace *upsert*. El `alertId` es determinista (se calcula con el tipo,
la zona y el evento ancla), así que un reintento no duplica la alerta. El topic tiene 3 particiones y la
llave es la zona: la zona N va a la partición (N − 1) mod 3.

### 3.1 Robo en curso CRITICAL (fila 1 de la tabla 2.4)

**Topic:** `alerts` · **Lo produce:** correlación (B o *worker*) · **Llave (zona):** `4` · **Partición:** 0

```json
{
  "alertId": "74961b2c-de7a-37fb-a59e-92ed99fe9cab",
  "revision": 1,
  "ruleId": "R1-ROBO",
  "alertType": "ROBO_EN_CURSO",
  "severity": "CRITICAL",
  "status": "NEW",
  "zoneId": 4,
  "zoneName": "Zona 4",
  "latitude": 14.619,
  "longitude": -90.52,
  "confidence": 0.9,
  "title": "Robo en curso",
  "description": "Robo en curso en Zona 4. Puntaje 0.90 (umbral 0.70). Evidencia: Boton de panico (Boton de panico ROBO, +0.40); Exceso de velocidad o contravia (Sensor de velocidad EXCESO_VELOCIDAD 85.0 km/h, +0.30); Vehiculo a alta velocidad captado por LPR (Camara LPR EXCESO_VELOCIDAD 84.0 km/h placa XYZ***, +0.20).",
  "createdAt": "2026-10-31T18:19:45.745228281Z",
  "firstEventTime": "2026-10-31T18:19:34.198374565Z",
  "triggerEventTime": "2026-10-31T18:19:45.715620074Z",
  "evidence": [
    {
      "eventId": "a9005e1e-a84a-4bf2-9210-c3f2097076d4",
      "sensorType": "PANIC",
      "subtype": "ROBO",
      "sensorId": "APP-3a82d1c2f946",
      "eventTime": "2026-10-31T18:19:34.198374565Z",
      "weight": 0.4,
      "summary": "Boton de panico ROBO",
      "detail": "desde la app (IOS)"
    },
    {
      "eventId": "43c13d3f-fa6b-4029-8302-d444d562ad8d",
      "sensorType": "SPEED",
      "subtype": "EXCESO_VELOCIDAD",
      "sensorId": "VEL-Z04-02",
      "eventTime": "2026-10-31T18:19:45.715620074Z",
      "weight": 0.3,
      "summary": "Sensor de velocidad EXCESO_VELOCIDAD 85.0 km/h",
      "detail": "sentido este, limite 60 km/h"
    },
    {
      "eventId": "a5c54d46-a5cf-4fc8-8f49-e8b7add7a547",
      "sensorType": "LPR",
      "subtype": "EXCESO_VELOCIDAD",
      "sensorId": "LPR-Z04-01",
      "eventTime": "2026-10-31T18:19:43.718360079Z",
      "weight": 0.2,
      "summary": "Camara LPR EXCESO_VELOCIDAD 84.0 km/h placa XYZ***",
      "detail": "Hyundai Tucson gris"
    }
  ],
  "plateMasked": "XYZ***",
  "plateHash": "a8376828ab99020a5483a6face13db17",
  "parentAlertId": null
}
```
_Pánico (+0,40) + cámara LPR a alta velocidad (+0,20) + exceso de velocidad (+0,30) = 0,90 → CRITICAL. Cada evidencia lleva el `eventId` del evento original._

### 3.2 Robo que escala de HIGH a CRITICAL (revisión 2)

**Topic:** `alerts` · **Lo produce:** correlación (B o *worker*) · **Llave (zona):** `6` · **Partición:** 2

```json
{
  "alertId": "229db468-10a1-3189-8851-0ba428c1b8ef",
  "revision": 2,
  "ruleId": "R1-ROBO",
  "alertType": "ROBO_EN_CURSO",
  "severity": "CRITICAL",
  "status": "ESCALATED",
  "zoneId": 6,
  "zoneName": "Zona 6",
  "latitude": 14.653,
  "longitude": -90.494,
  "confidence": 0.9,
  "title": "Robo en curso",
  "description": "Robo en curso en Zona 6. Puntaje 0.90 (umbral 0.70). Evidencia: Boton de panico (Boton de panico ROBO, +0.40); Exceso de velocidad o contravia (Sensor de velocidad EXCESO_VELOCIDAD 85.0 km/h, +0.30); Vehiculo a alta velocidad captado por LPR (Camara LPR EXCESO_VELOCIDAD 87.0 km/h placa XYZ***, +0.20).",
  "createdAt": "2026-10-31T18:19:52.334695878Z",
  "firstEventTime": "2026-10-31T18:19:34.524454853Z",
  "triggerEventTime": "2026-10-31T18:19:52.246901191Z",
  "evidence": [
    {
      "eventId": "a487b916-013b-400b-b1c4-9262df63da9b",
      "sensorType": "PANIC",
      "subtype": "ROBO",
      "sensorId": "APP-d980e0df83f7",
      "eventTime": "2026-10-31T18:19:34.524454853Z",
      "weight": 0.4,
      "summary": "Boton de panico ROBO",
      "detail": "desde la app (IOS)"
    },
    {
      "eventId": "c5de7f05-3824-4b39-8df1-da8f81503e8a",
      "sensorType": "SPEED",
      "subtype": "EXCESO_VELOCIDAD",
      "sensorId": "VEL-Z06-02",
      "eventTime": "2026-10-31T18:19:49.246970900Z",
      "weight": 0.3,
      "summary": "Sensor de velocidad EXCESO_VELOCIDAD 85.0 km/h",
      "detail": "sentido sur, limite 60 km/h"
    },
    {
      "eventId": "2ca50b41-71d0-4270-b2a3-071036880eba",
      "sensorType": "LPR",
      "subtype": "EXCESO_VELOCIDAD",
      "sensorId": "LPR-Z06-01",
      "eventTime": "2026-10-31T18:19:52.246901191Z",
      "weight": 0.2,
      "summary": "Camara LPR EXCESO_VELOCIDAD 87.0 km/h placa XYZ***",
      "detail": "Mitsubishi L200 blanco"
    }
  ],
  "plateMasked": "XYZ***",
  "plateHash": "a8376828ab99020a5483a6face13db17",
  "parentAlertId": null
}
```
_Mismo `alertId` que la revisión 1 (HIGH, 0,70); al llegar la cámara LPR sube a CRITICAL y el estado pasa a `ESCALATED`._

### 3.3 Vehículo en fuga (vinculado al robo original)

**Topic:** `alerts` · **Lo produce:** correlación (B o *worker*) · **Llave (zona):** `6` · **Partición:** 2

```json
{
  "alertId": "e8407448-d407-39ab-9c27-0cb6def79ed2",
  "revision": 1,
  "ruleId": "R8-SEGUIMIENTO-PLACA",
  "alertType": "VEHICULO_EN_FUGA",
  "severity": "CRITICAL",
  "status": "NEW",
  "zoneId": 6,
  "zoneName": "Zona 6",
  "latitude": 14.6493,
  "longitude": -90.499,
  "confidence": 0.9,
  "title": "Vehiculo sospechoso en fuga",
  "description": "Vehiculo placa XYZ*** vinculado al incidente ROBO_EN_CURSO de Zona 4 detectado en Zona 6 a 87 km/h, 0 min 6 s despues.",
  "createdAt": "2026-10-31T18:19:52.272483671Z",
  "firstEventTime": "2026-10-31T18:19:52.246901191Z",
  "triggerEventTime": "2026-10-31T18:19:52.246901191Z",
  "evidence": [
    {
      "eventId": "2ca50b41-71d0-4270-b2a3-071036880eba",
      "sensorType": "LPR",
      "subtype": "EXCESO_VELOCIDAD",
      "sensorId": "LPR-Z06-01",
      "eventTime": "2026-10-31T18:19:52.246901191Z",
      "weight": 0.9,
      "summary": "Camara LPR EXCESO_VELOCIDAD 87.0 km/h placa XYZ***",
      "detail": "Mitsubishi L200 blanco"
    }
  ],
  "plateMasked": "XYZ***",
  "plateHash": "a8376828ab99020a5483a6face13db17",
  "parentAlertId": "74961b2c-de7a-37fb-a59e-92ed99fe9cab"
}
```
_La placa del robo de la Zona 4 apareció en la Zona 6. `parentAlertId` apunta a la alerta 3.1._

### 3.4 Posible falso positivo (pánico de la app sin corroborar)

**Topic:** `alerts` · **Lo produce:** correlación (B o *worker*) · **Llave (zona):** `9` · **Partición:** 2

```json
{
  "alertId": "772b288d-9895-3432-92f6-b773cf2a217e",
  "revision": 1,
  "ruleId": "R1-ROBO/sin-corroborar",
  "alertType": "POSIBLE_FALSO_POSITIVO",
  "severity": "LOW",
  "status": "AUTO_CLASSIFIED",
  "zoneId": 9,
  "zoneName": "Zona 9",
  "latitude": 14.604,
  "longitude": -90.516,
  "confidence": 0.4,
  "title": "Posible falso positivo: robo en curso sin corroborar",
  "description": "Boton de panico sin eventos complementarios en Zona 9 durante 90 s. Puntaje 0.40 < umbral 0.70. Evidencia: Boton de panico (Boton de panico ROBO, +0.40).",
  "createdAt": "2026-10-31T18:21:06.581746915Z",
  "firstEventTime": "2026-10-31T18:19:35Z",
  "triggerEventTime": "2026-10-31T18:19:35Z",
  "evidence": [
    {
      "eventId": "edf5c3f8-0bae-4d44-a826-61b43c73d95a",
      "sensorType": "PANIC",
      "subtype": "ROBO",
      "sensorId": "APP-ddfbac30c201",
      "eventTime": "2026-10-31T18:19:35Z",
      "weight": 0.4,
      "summary": "Boton de panico ROBO",
      "detail": "desde la app (ANDROID)"
    }
  ],
  "plateMasked": null,
  "plateHash": null,
  "parentAlertId": null
}
```
_Es el pánico de 1.1 / 2.1: en 90 s no llegó nada que lo corrobore (0,40 < 0,70)._

**Valores de `severity` y `status`:**

| Campo | Valores |
|---|---|
| `severity` | `LOW`, `MEDIUM`, `HIGH`, `CRITICAL` |
| `status` en Kafka | `NEW` (primer aviso), `ESCALATED` (subió la severidad o apareció la placa), `UPDATED` (se sumó otro testigo), `AUTO_CLASSIFIED` (posible falso positivo) |
| `status` tras validar (consola) | `CONFIRMED` o `FALSE_POSITIVE` |

## 4. Cola de mensajes muertos, DLQ (Kafka)

Lo que no cumple el contrato va a `events.dlq` **sin reintentos** (falla PERMANENTE). El cuerpo es el
mensaje original, intacto; la causa viaja en cabeceras. Las cabeceras `kafka_dlt-original-partition`,
`-offset` y `-timestamp` van en binario y `kafka_dlt-exception-stacktrace` trae la traza completa; no se
muestran aquí.

### 4.1 JSON sin los campos obligatorios

**Topic:** `events.dlq` · **Lo produce:** ingesta (B) · **Llave:** ninguna · **Partición:** 0

Cuerpo (el mensaje original):

```json
{
  "tipo_alerta": "ROBO",
  "zona_geografica": {
    "nombre": "Zona 3"
  }
}
```

Cabeceras:

| Cabecera | Valor |
|---|---|
| `kafka_dlt-original-topic` | `sensor.panic.raw` |
| `kafka_dlt-original-consumer-group` | `ingestion` |
| `kafka_dlt-exception-fqcn` | `org.springframework.kafka.listener.ListenerExecutionFailedException` |
| `kafka_dlt-exception-cause-fqcn` | `gt.umg.centinela.common.json.InvalidEventException` |
| `kafka_dlt-exception-message` | `Listener method 'public void gt.umg.centinela.ingestion.pipeline.RawEventListener.panic(org.apache.kafka.clients.consumer.ConsumerRecord<java.lang.String, java.lang.String>)' threw exception; Contrato invalido de PanicButtonEvent: deviceId must not be blank; eventId must not be null; timestamp must not be null` |
| `centinela-error-class` | `PERMANENTE` |
| `centinela-service` | `ingestion-service` |

_Se inyectó a mano con `kafka-console-producer`._

### 4.2 Pánico con una zona que no existe (enviado por la app)

**Topic:** `events.dlq` · **Lo produce:** ingesta (B) · **Llave:** `APP-58213` · **Partición:** 0

Cuerpo (el mensaje original):

```json
{
  "event_id": "93004198-9c87-48a3-b619-40b93843ef08",
  "timestamp": "2026-10-31T18:20:10Z",
  "zona_geografica": {
    "nombre": "Zona 99",
    "latitud": null,
    "longitud": null
  },
  "tipo_alerta": "ROBO",
  "identificador_dispositivo": "APP-58213",
  "user_context": null
}
```

Cabeceras:

| Cabecera | Valor |
|---|---|
| `kafka_dlt-original-topic` | `sensor.panic.raw` |
| `kafka_dlt-original-consumer-group` | `ingestion` |
| `kafka_dlt-exception-fqcn` | `org.springframework.kafka.listener.ListenerExecutionFailedException` |
| `kafka_dlt-exception-cause-fqcn` | `gt.umg.centinela.common.json.InvalidEventException` |
| `kafka_dlt-exception-message` | `Listener method 'public void gt.umg.centinela.ingestion.pipeline.RawEventListener.panic(org.apache.kafka.clients.consumer.ConsumerRecord<java.lang.String, java.lang.String>)' threw exception; Zona desconocida: Zona 99` |
| `centinela-error-class` | `PERMANENTE` |
| `centinela-service` | `ingestion-service` |
| `centinela-first-attempt` | `1793470810981 (2026-10-31 18:20:10.981 UTC)` |

_El gateway lo aceptó (revisa el formato y los campos obligatorios); la ingesta revisó la zona, la rechazó y lo envió a la DLQ._

## 5. Gateway de la app (HTTP, nodo B)

Puerta de entrada de la app móvil: `ingestion-service`, puerto 8081. Revisa el formato y los campos
obligatorios (400 si fallan); la zona y la hora se revisan después, en la ingesta (si fallan, el evento
termina en la DLQ, ver 4.2). Solo responde 202 cuando el evento ya está guardado en Kafka o en disco.

### 5.1 Pánico enviado por la app (petición)

**POST** `http://IP_B:8081/api/v1/panic`

```json
{
  "event_id": "edf5c3f8-0bae-4d44-a826-61b43c73d95a",
  "timestamp": "2026-10-31T18:19:35Z",
  "zona_geografica": {
    "nombre": "Zona 9",
    "latitud": 14.604,
    "longitud": -90.516
  },
  "tipo_alerta": "ROBO",
  "identificador_dispositivo": "APP-58213",
  "user_context": {
    "usuario_id": "CIU-58213",
    "plataforma": "ANDROID",
    "version_app": "2.3.1"
  }
}
```

### 5.2 Respuesta: recibido por Kafka

**HTTP 202** Accepted

```json
{
  "event_id": "edf5c3f8-0bae-4d44-a826-61b43c73d95a",
  "estado": "RECIBIDO"
}
```

### 5.3 Respuesta: guardado en el spool con Kafka caído

**HTTP 202** Accepted, a los 3,0 s (el plazo de espera de Kafka es de 3 s)

Petición (pánico ROBO desde un iPhone, Zona 8):

```json
{
  "event_id": "0ced83f1-9fee-419c-82df-e48c5dbd3833",
  "timestamp": "2026-10-31T18:30:55Z",
  "zona_geografica": {
    "nombre": "Zona 8",
    "latitud": 14.6301,
    "longitud": -90.5389
  },
  "tipo_alerta": "ROBO",
  "identificador_dispositivo": "APP-30417",
  "user_context": {
    "usuario_id": "CIU-30417",
    "plataforma": "IOS",
    "version_app": "2.3.1"
  }
}
```

Respuesta:

```json
{
  "event_id": "0ced83f1-9fee-419c-82df-e48c5dbd3833",
  "estado": "EN_COLA"
}
```

### 5.4 Respuesta: sin Kafka ni disco disponible

**HTTP 503** Service Unavailable, con la cabecera `Retry-After: 10`

```json
{
  "event_id": "<el event_id del pánico>",
  "estado": "NO_DISPONIBLE",
  "error": "No se pudo guardar el evento; reintente con el mismo event_id"
}
```
_Formato tomado del código (`AppGatewayController`): no se provocó en la prueba porque exige que el disco del spool esté lleno o falle._

### 5.5 Respuesta: contrato inválido (falta un campo obligatorio)

**HTTP 400** Bad Request

Petición: un pánico sin `identificador_dispositivo`. Respuesta:

```json
{
  "error": "Contrato invalido de PanicButtonEvent: deviceId must not be blank"
}
```

### 5.6 Respuesta: JSON que no se puede leer (valor no permitido)

**HTTP 400** Bad Request

Petición: `"tipo_alerta": "CHISME"`, que no es un valor permitido. Responde Spring Boot con su formato estándar:

```json
{
  "timestamp": "2026-10-31T18:19:35.859+00:00",
  "status": 400,
  "error": "Bad Request",
  "path": "/api/v1/panic"
}
```

### 5.7 Reporte ciudadano enviado por la app (petición)

**POST** `http://IP_B:8081/api/v1/citizen-reports`

```json
{
  "event_id": "a8cbf043-aec1-4c7e-b00a-e2fae12e2f2b",
  "timestamp": "2026-10-31T18:19:35Z",
  "tipo_evento": "ACCIDENTE",
  "mensaje_descriptivo": "Choque entre dos carros frente al mercado, el de placa P512KLM se dio a la fuga",
  "ubicacion_aproximada": {
    "latitud": 14.6105,
    "longitud": -90.529
  },
  "origen": "APP",
  "reportante_id": "CIU-77120"
}
```

Respuesta (**HTTP 202**):

```json
{
  "event_id": "a8cbf043-aec1-4c7e-b00a-e2fae12e2f2b",
  "estado": "RECIBIDO"
}
```

### 5.8 Estado del gateway en operación normal

**GET** `http://IP_B:8081/api/v1/gateway/status`

```json
{
  "kafkaDisponible": true,
  "spoolPendientes": 0,
  "enVuelo": 0,
  "reenviados": 0,
  "apartados": 0,
  "rechazadosPorTope": 0,
  "lineasInvalidas": 0,
  "ultimoError": null
}
```

### 5.9 Estado del gateway con Kafka caído

**GET** `http://IP_B:8081/api/v1/gateway/status`

```json
{
  "kafkaDisponible": false,
  "spoolPendientes": 1,
  "enVuelo": 0,
  "reenviados": 0,
  "apartados": 0,
  "rechazadosPorTope": 0,
  "lineasInvalidas": 0,
  "ultimoError": null
}
```
_El pánico de 5.3 espera en el `gateway.wal`._

### 5.10 Estado del gateway después de recuperar Kafka

**GET** `http://IP_B:8081/api/v1/gateway/status`

```json
{
  "kafkaDisponible": true,
  "spoolPendientes": 0,
  "enVuelo": 0,
  "reenviados": 1,
  "apartados": 0,
  "rechazadosPorTope": 0,
  "lineasInvalidas": 0,
  "ultimoError": "TimeoutException: Topic sensor.panic.raw not present in metadata after 3000 ms."
}
```
_Se reenvió solo (`reenviados: 1`). `ultimoError` conserva la última falla, de cuando Kafka no respondía._

## 6. API de la consola: alert-service (HTTP, nodo B)

Puerto 8083, con usuario y contraseña (HTTP Basic). Las consultas piden el rol OPERATOR; las de
administración, ADMIN. La consola muestra las alertas en un formato propio (`AlertView`): agrega la
latencia y los datos de validación, y **no incluye `plateHash`** (el operador solo ve la placa enmascarada).

### 6.1 Lista de alertas

**GET** `http://IP_B:8083/api/alerts?limit=200` · rol OPERATOR

```json
[
  {
    "alertId": "e8407448-d407-39ab-9c27-0cb6def79ed2",
    "revision": 1,
    "ruleId": "R8-SEGUIMIENTO-PLACA",
    "alertType": "VEHICULO_EN_FUGA",
    "severity": "CRITICAL",
    "status": "NEW",
    "zoneId": 6,
    "zoneName": "Zona 6",
    "latitude": 14.6493,
    "longitude": -90.499,
    "confidence": 0.9,
    "title": "Vehiculo sospechoso en fuga",
    "description": "Vehiculo placa XYZ*** vinculado al incidente ROBO_EN_CURSO de Zona 4 detectado en Zona 6 a 87 km/h, 0 min 6 s despues.",
    "createdAt": "2026-10-31T18:19:52.272484Z",
    "updatedAt": "2026-10-31T18:19:52.272484Z",
    "triggerEventTime": "2026-10-31T18:19:52.246901Z",
    "latencyMs": 38,
    "plateMasked": "XYZ***",
    "evidence": [
      {
        "detail": "Mitsubishi L200 blanco",
        "weight": 0.9,
        "eventId": "2ca50b41-71d0-4270-b2a3-071036880eba",
        "subtype": "EXCESO_VELOCIDAD",
        "summary": "Camara LPR EXCESO_VELOCIDAD 87.0 km/h placa XYZ***",
        "sensorId": "LPR-Z06-01",
        "eventTime": "2026-10-31T18:19:52.246901191Z",
        "sensorType": "LPR"
      }
    ],
    "parentAlertId": "74961b2c-de7a-37fb-a59e-92ed99fe9cab",
    "validatedBy": null,
    "validatedAt": null,
    "resolutionNotes": null
  }
]
```
_Devuelve una lista; se muestra 1 de 19 alertas._

### 6.2 Detalle de una alerta con su evidencia

**GET** `http://IP_B:8083/api/alerts/{id}` · rol OPERATOR

```json
{
  "alertId": "74961b2c-de7a-37fb-a59e-92ed99fe9cab",
  "revision": 1,
  "ruleId": "R1-ROBO",
  "alertType": "ROBO_EN_CURSO",
  "severity": "CRITICAL",
  "status": "NEW",
  "zoneId": 4,
  "zoneName": "Zona 4",
  "latitude": 14.619,
  "longitude": -90.52,
  "confidence": 0.9,
  "title": "Robo en curso",
  "description": "Robo en curso en Zona 4. Puntaje 0.90 (umbral 0.70). Evidencia: Boton de panico (Boton de panico ROBO, +0.40); Exceso de velocidad o contravia (Sensor de velocidad EXCESO_VELOCIDAD 85.0 km/h, +0.30); Vehiculo a alta velocidad captado por LPR (Camara LPR EXCESO_VELOCIDAD 84.0 km/h placa XYZ***, +0.20).",
  "createdAt": "2026-10-31T18:19:45.745228Z",
  "updatedAt": "2026-10-31T18:19:45.745228Z",
  "triggerEventTime": "2026-10-31T18:19:45.715620Z",
  "latencyMs": 34,
  "plateMasked": "XYZ***",
  "evidence": [
    {
      "detail": "desde la app (IOS)",
      "weight": 0.4,
      "eventId": "a9005e1e-a84a-4bf2-9210-c3f2097076d4",
      "subtype": "ROBO",
      "summary": "Boton de panico ROBO",
      "sensorId": "APP-3a82d1c2f946",
      "eventTime": "2026-10-31T18:19:34.198374565Z",
      "sensorType": "PANIC"
    },
    {
      "detail": "sentido este, limite 60 km/h",
      "weight": 0.3,
      "eventId": "43c13d3f-fa6b-4029-8302-d444d562ad8d",
      "subtype": "EXCESO_VELOCIDAD",
      "summary": "Sensor de velocidad EXCESO_VELOCIDAD 85.0 km/h",
      "sensorId": "VEL-Z04-02",
      "eventTime": "2026-10-31T18:19:45.715620074Z",
      "sensorType": "SPEED"
    },
    {
      "detail": "Hyundai Tucson gris",
      "weight": 0.2,
      "eventId": "a5c54d46-a5cf-4fc8-8f49-e8b7add7a547",
      "subtype": "EXCESO_VELOCIDAD",
      "summary": "Camara LPR EXCESO_VELOCIDAD 84.0 km/h placa XYZ***",
      "sensorId": "LPR-Z04-01",
      "eventTime": "2026-10-31T18:19:43.718360079Z",
      "sensorType": "LPR"
    }
  ],
  "parentAlertId": null,
  "validatedBy": null,
  "validatedAt": null,
  "resolutionNotes": null
}
```
_Es la alerta 3.1 vista desde la consola: `latencyMs` es la latencia evento → alerta (34 ms)._

### 6.3 Evento original de una evidencia (trazabilidad)

**GET** `http://IP_B:8083/api/events/{id}?at={hora del evento}` · rol OPERATOR

```json
{
  "event_id": "a5c54d46-a5cf-4fc8-8f49-e8b7add7a547",
  "event_time": "2026-10-31T18:19:43.718360Z",
  "ingested_at": "2026-10-31T18:19:43.728991Z",
  "stored_at": "2026-10-31T18:19:43.736341Z",
  "sensor_type": "LPR",
  "sensor_id": "LPR-Z04-01",
  "zone_id": 4,
  "zone_name": "Zona 4",
  "latitude": 14.622100000000001,
  "longitude": -90.5191,
  "subtype": "EXCESO_VELOCIDAD",
  "value": 84.0,
  "confidence": 1.0,
  "anomalous": true,
  "plate_masked": "XYZ***",
  "origin": "SENSOR",
  "attributes": {
    "color": "Gris",
    "modelo": "Hyundai Tucson"
  }
}
```
_Es la lectura LPR de 1.4 / 2.3 tal como quedó guardada en TimescaleDB: los nombres salen en `snake_case` porque vienen de la base, y los seudónimos no se exponen._

### 6.4 Validación del operador (petición)

**PATCH** `http://IP_B:8083/api/alerts/{id}/validation` · rol OPERATOR

```json
{
  "resolution": "FALSE_POSITIVE",
  "notes": "Se llamo al usuario: presiono el boton por error"
}
```
_`resolution` acepta `CONFIRMED` (era real) o `FALSE_POSITIVE` (no lo era)._

### 6.5 Alerta después de validarla

Respuesta del PATCH anterior · **HTTP 200**

```json
{
  "alertId": "772b288d-9895-3432-92f6-b773cf2a217e",
  "revision": 1,
  "ruleId": "R1-ROBO/sin-corroborar",
  "alertType": "POSIBLE_FALSO_POSITIVO",
  "severity": "LOW",
  "status": "FALSE_POSITIVE",
  "zoneId": 9,
  "zoneName": "Zona 9",
  "latitude": 14.604,
  "longitude": -90.516,
  "confidence": 0.4,
  "title": "Posible falso positivo: robo en curso sin corroborar",
  "description": "Boton de panico sin eventos complementarios en Zona 9 durante 90 s. Puntaje 0.40 < umbral 0.70. Evidencia: Boton de panico (Boton de panico ROBO, +0.40).",
  "createdAt": "2026-10-31T18:21:06.581747Z",
  "updatedAt": "2026-10-31T18:21:06.581747Z",
  "triggerEventTime": "2026-10-31T18:19:35Z",
  "latencyMs": 91585,
  "plateMasked": null,
  "evidence": [
    {
      "detail": "desde la app (ANDROID)",
      "weight": 0.4,
      "eventId": "edf5c3f8-0bae-4d44-a826-61b43c73d95a",
      "subtype": "ROBO",
      "summary": "Boton de panico ROBO",
      "sensorId": "APP-ddfbac30c201",
      "eventTime": "2026-10-31T18:19:35Z",
      "sensorType": "PANIC"
    }
  ],
  "parentAlertId": null,
  "validatedBy": "operador",
  "validatedAt": "2026-10-31T18:28:37.413783Z",
  "resolutionNotes": "Se llamo al usuario: presiono el boton por error"
}
```
_Es la alerta 3.4: queda registrado quién la validó, cuándo y por qué. Estas validaciones alimentan el ajuste adaptativo de umbrales._

### 6.6 Alertas en vivo (Server-Sent Events)

**GET** `http://IP_B:8083/api/alerts/stream` · rol OPERATOR

Así llega el flujo a la consola: un comentario al conectarse y luego un evento `alert` por cada alerta
(`data` trae la alerta en el mismo formato que 6.2, en una sola línea; aquí se abrevia):

```text
:conectado

event:alert
data:{"alertId":"a7d3f2f5-7508-3347-966e-cbc4eb6efeed","revision":1,"ruleId":"R7-EMERGENCIA-MEDICA","alertType":"EMERGENCIA_MEDICA","severity":"HIGH",...
```

El primer `data` completo:

```json
{
  "alertId": "a7d3f2f5-7508-3347-966e-cbc4eb6efeed",
  "revision": 1,
  "ruleId": "R7-EMERGENCIA-MEDICA",
  "alertType": "EMERGENCIA_MEDICA",
  "severity": "HIGH",
  "status": "NEW",
  "zoneId": 12,
  "zoneName": "Zona 12",
  "latitude": 14.58437380681935,
  "longitude": -90.53374900039985,
  "confidence": 1.0,
  "title": "Emergencia medica",
  "description": "Emergencia medica en Zona 12. Puntaje 1.00 (umbral 0.50). Evidencia: Boton de emergencia medica (Boton de panico EMERGENCIA_MEDICA, +1.00).",
  "createdAt": "2026-10-31T18:26:17.844832Z",
  "updatedAt": "2026-10-31T18:26:17.844832Z",
  "triggerEventTime": "2026-10-31T18:26:17.797271Z",
  "latencyMs": 55,
  "plateMasked": null,
  "evidence": [
    {
      "detail": "boton fisico",
      "weight": 1.0,
      "eventId": "6fa2a76b-3f12-423d-8135-28ee52bacb50",
      "subtype": "EMERGENCIA_MEDICA",
      "summary": "Boton de panico EMERGENCIA_MEDICA",
      "sensorId": "BTN-Z12-05",
      "eventTime": "2026-10-31T18:26:17.797270799Z",
      "sensorType": "PANIC"
    }
  ],
  "parentAlertId": null,
  "validatedBy": null,
  "validatedAt": null,
  "resolutionNotes": null
}
```

### 6.7 Resumen para el encabezado de la consola

**GET** `http://IP_B:8083/api/stats/summary?minutes=60` · rol OPERATOR

```json
{
  "total": 19,
  "critical": 8,
  "high": 5,
  "open": 13,
  "possible_false_positives": 6,
  "validated_false_positives": 0,
  "confirmed": 0,
  "p95_latency_ms": 53.5,
  "windowMinutes": 60,
  "latencyWindowMinutes": 5,
  "connectedConsoles": 1
}
```
_`p95_latency_ms` usa la ventana del SLO-1 (`latencyWindowMinutes`: 5 min)._

### 6.8 Catálogo de zonas

**GET** `http://IP_B:8083/api/zones` · rol OPERATOR

```json
[
  {
    "id": 1,
    "name": "Zona 1",
    "latitude": 14.642,
    "longitude": -90.513,
    "row": 0,
    "col": 0
  },
  {
    "id": 2,
    "name": "Zona 2",
    "latitude": 14.6595,
    "longitude": -90.512,
    "row": 0,
    "col": 1
  }
]
```
_Se muestran 2 de 12 zonas._

### 6.9 Configuración de la consola

**GET** `http://IP_B:8083/api/console/config` · rol OPERATOR

```json
{
  "grafanaPort": 3000
}
```

### 6.10 Lista de placas robadas

**GET** `http://IP_B:8083/api/watchlist` · rol OPERATOR

```json
[
  {
    "plate_masked": "M04***",
    "reason": "Reporte de robo (carga inicial)",
    "added_at": "2026-10-15T13:26:30.425+00:00",
    "added_by": "sistema"
  },
  {
    "plate_masked": "P77***",
    "reason": "Reporte de robo (carga inicial)",
    "added_at": "2026-10-15T13:26:30.420+00:00",
    "added_by": "sistema"
  }
]
```
_Se muestran 2 de 6. La base solo guarda el seudónimo y la placa enmascarada._

### 6.11 Agregar una placa robada (petición y respuesta)

**POST** `http://IP_B:8083/api/watchlist` · rol ADMIN

```json
{
  "plate": "P999ZZZ",
  "reason": "Denuncia de robo 2026-1031"
}
```

Respuesta (**HTTP 201** Created):

```json
{
  "added": true,
  "plate_masked": "P99***"
}
```
_Para quitarla: `DELETE /api/watchlist/{placa}` (responde HTTP 204, sin cuerpo)._

### 6.12 Resumen de la DLQ

**GET** `http://IP_B:8083/api/admin/dlq?ultimos=15` · rol ADMIN

```json
{
  "total": 5,
  "porOrigenClaseYServicio": {
    "sensor.panic.raw / PERMANENTE / ingestion-service": 5
  },
  "ultimos": [
    {
      "hora": "2026-10-31T18:20:10.986Z",
      "topicOriginal": "sensor.panic.raw",
      "clase": "PERMANENTE",
      "servicio": "ingestion-service",
      "reprocesos": 0,
      "causa": "Zona desconocida: Zona 99"
    },
    {
      "hora": "2026-10-31T18:19:44.032Z",
      "topicOriginal": "sensor.panic.raw",
      "clase": "PERMANENTE",
      "servicio": "ingestion-service",
      "reprocesos": 0,
      "causa": "Zona desconocida: Zona 99"
    }
  ]
}
```
_Se muestran 2 de los 3 últimos. Es lo que imprime `centinela.cmd dlq`._

### 6.13 Reprocesar la DLQ

**POST** `http://IP_B:8083/api/admin/dlq/reprocess?incluirPermanentes=false` · rol ADMIN

```json
{
  "reprocessed": 0,
  "skipped": 0,
  "read": 0,
  "incluirPermanentes": false
}
```
_Por defecto no reinyecta los PERMANENTE (mensajes mal formados): reintentarlos no los arreglaría._

### 6.14 Ejecutar ya una tarea periódica

**POST** `http://IP_B:8083/api/admin/jobs/rule-quality` · rol ADMIN

```json
{
  "job": "rule-quality",
  "executedAt": "2026-10-31T18:28:38.198165736Z"
}
```
_Tareas: `rule-quality`, `slo`, `plate-retention`, `alert-expiry`, `watchlist`._

### 6.15 Limpiar el estado de correlación de una zona

**POST** `http://IP_B:8083/api/admin/correlation/reset?zonas=1` · rol ADMIN

```json
{
  "executedAt": "2026-10-31T18:28:38.429274995Z",
  "detail": {
    "win:1:*": 0,
    "incident:*:1": 0
  },
  "deletedKeys": 0
}
```

### 6.16 Informe de eventos críticos perdidos (SLO-2)

**POST** `http://IP_B:8083/api/admin/slo/lost-critical-events` · rol ADMIN

```json
{
  "node": "nodo-c",
  "lost": 1
}
```
_Lo envía el simulador si un evento crítico no se pudo guardar ni enviar; genera una alerta CRITICAL en la consola._

Respuesta:

```json
{
  "registrado": true,
  "perdidos": 1
}
```

### 6.17 Salud de un servicio

**GET** `http://IP_B:8081/actuator/health` (igual en los 4 servicios)

```json
{
  "status": "UP",
  "groups": [
    "liveness",
    "readiness"
  ],
  "components": {
    "diskSpace": {
      "status": "UP"
    },
    "livenessState": {
      "status": "UP"
    },
    "ping": {
      "status": "UP"
    },
    "readinessState": {
      "status": "UP"
    },
    "redis": {
      "status": "UP"
    },
    "ssl": {
      "status": "UP"
    }
  }
}
```
_Es lo que consulta el vigilante para decidir si el nodo B está OPERATIVO._

## 7. API del simulador (HTTP, nodos C/D)

Puerto 8080, sin usuario. Es lo que usan `centinela.cmd escenario`, `pico`, `fondo`, `replay` y `validar`.

### 7.1 Estado del simulador

**GET** `http://IP_C:8080/api/simulator/status`

```json
{
  "node": "nodo-c",
  "zones": [
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12
  ],
  "baselineEnabled": true,
  "bursting": false,
  "multiplier": 1.0,
  "expectedEventsPerSecond": 13.48,
  "ratesPerZone": {
    "PANIC": 0.001,
    "LPR": 0.6,
    "SPEED": 0.5,
    "ACOUSTIC": 0.02,
    "CITIZEN_REPORT": 0.002
  },
  "spoolPending": 0,
  "spoolTelemetryPending": 0,
  "nodeB": "OPERATIVO",
  "quietZones": {
    "2": "2026-10-31T18:25:29.573325653Z",
    "4": "2026-10-31T18:25:15.714809696Z",
    "6": "2026-10-31T18:27:34.246288111Z",
    "7": "2026-10-31T18:25:24.894307857Z",
    "9": "2026-10-31T18:27:34.246288111Z",
    "10": "2026-10-31T18:27:34.246288111Z",
    "11": "2026-10-31T18:26:05.060060132Z",
    "12": "2026-10-31T18:26:05.060060132Z"
  }
}
```
_`expectedEventsPerSecond`: 13,48 eventos/s en las 12 zonas. `quietZones`: zonas aisladas del ruido de fondo mientras corre un escenario._

### 7.2 Lista de escenarios

**GET** `http://IP_C:8080/api/scenarios`

```json
[
  {
    "id": "robo-en-curso",
    "title": "Robo en curso con vehiculo en fuga",
    "description": "Boton de panico (ROBO) + exceso de velocidad + camara LPR con la placa XYZ123 a alta velocidad. Luego la misma placa aparece en zonas vecinas. Se espera ROBO_EN_CURSO (escala a CRITICAL) y VEHICULO_EN_FUGA en la siguiente zona.",
    "defaultZone": 4,
    "includeInValidation": true,
    "fixedZone": false
  },
  {
    "id": "falso-positivo",
    "title": "Boton de panico sin eventos complementarios",
    "description": "Un unico boton de panico sin ninguna otra senal en la zona. Tras el tiempo de corroboracion se espera POSIBLE_FALSO_POSITIVO (fila 2 de la tabla 2.4).",
    "defaultZone": 10,
    "includeInValidation": true,
    "fixedZone": false
  }
]
```
_Se muestran 2 de 10 escenarios._

### 7.3 Lanzar un escenario (tabla 2.4)

**POST** `http://IP_C:8080/api/scenarios/tabla-2-4?speed=1`

```json
{
  "executionId": "a3fe5ada-134f-4212-996b-a919d5ad07a1",
  "scenarioId": "tabla-2-4",
  "title": "Ejemplo exacto de la tabla 2.4 del enunciado",
  "zoneId": 4,
  "startedAt": "2026-10-31T18:19:33.714809696Z",
  "steps": [
    {
      "at": "2026-10-31T18:19:33.714809696Z",
      "description": "Fila 1: Boton de panico en Zona 4"
    },
    {
      "at": "2026-10-31T18:19:33.714809696Z",
      "description": "Fila 2: Boton de panico en Zona 10"
    },
    {
      "at": "2026-10-31T18:19:43.714809696Z",
      "description": "Fila 1: Lectura LPR placa XYZ*** en Zona 4"
    },
    {
      "at": "2026-10-31T18:19:45.714809696Z",
      "description": "Fila 1: Sensor de velocidad 85 km/h en Zona 4"
    }
  ],
  "expected": [
    {
      "alertType": "ROBO_EN_CURSO",
      "zoneId": 4,
      "withinSeconds": 30,
      "minSeverity": "CRITICAL"
    },
    {
      "alertType": "POSIBLE_FALSO_POSITIVO",
      "zoneId": 10,
      "withinSeconds": 150,
      "minSeverity": "LOW"
    }
  ],
  "quietZones": [
    4,
    10
  ],
  "quietUntil": "2026-10-31T18:25:15.714809696Z",
  "quietSeconds": 342,
  "isolation": "Zonas 4, 10 sin ruido de fondo hasta las 12:25:15; historial reciente e incidentes abiertos de 4, 10 reiniciados."
}
```
_Incluye los pasos, las alertas esperadas y las zonas aisladas del ruido de fondo._

### 7.4 Pico de carga

**POST** `http://IP_C:8080/api/simulator/burst?multiplier=2&seconds=5`

```json
{
  "node": "nodo-c",
  "zones": [
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12
  ],
  "baselineEnabled": true,
  "bursting": true,
  "multiplier": 2.0,
  "expectedEventsPerSecond": 26.95,
  "ratesPerZone": {
    "PANIC": 0.001,
    "LPR": 0.6,
    "SPEED": 0.5,
    "ACOUSTIC": 0.02,
    "CITIZEN_REPORT": 0.002
  },
  "spoolPending": 0,
  "spoolTelemetryPending": 0,
  "nodeB": "OPERATIVO",
  "quietZones": {
    "1": "2026-10-31T18:28:52.224288149Z",
    "2": "2026-10-31T18:32:28.808029270Z",
    "5": "2026-10-31T18:32:04.062910285Z"
  }
}
```
_Responde con el estado del simulador: `bursting` y `multiplier` muestran el pico activo. `baseline?enabled=` (tráfico de fondo) y `quiet?zones=&seconds=` (aislar zonas) responden con este mismo formato._

### 7.5 Replay de un CSV

**POST** `http://IP_C:8080/api/replay?resource=noche-de-viernes.csv&speed=10&jitter=true`

```json
{
  "source": "noche-de-viernes.csv",
  "events": 15,
  "durationSeconds": 26.487867283972967,
  "startedAt": "2026-10-31T18:28:47.262065072Z"
}
```

### 7.6 Iniciar la validación automática

**POST** `http://IP_C:8080/api/validation`

```json
{
  "id": "e6bf971f-1b52-4618-a7c6-1de5e539de30",
  "status": "RUNNING",
  "startedAt": "2026-10-31T18:28:55.708343568Z",
  "finishedAt": null,
  "passed": 0,
  "total": 9,
  "stateReset": "ok (79 llaves eliminadas)",
  "results": []
}
```

### 7.7 Resultado de la validación (9 de 9)

**GET** `http://IP_C:8080/api/validation/latest`

```json
{
  "id": "e6bf971f-1b52-4618-a7c6-1de5e539de30",
  "status": "PASSED",
  "startedAt": "2026-10-31T18:28:55.708343568Z",
  "finishedAt": "2026-10-31T18:30:28.620132724Z",
  "passed": 9,
  "total": 9,
  "stateReset": "ok (79 llaves eliminadas)",
  "results": [
    {
      "scenario": "robo-en-curso",
      "expectedAlert": "ROBO_EN_CURSO",
      "zoneId": 4,
      "minSeverity": "CRITICAL",
      "passed": true,
      "foundSeverity": "CRITICAL",
      "alertId": "1b114d45-9e48-32db-9841-76278ecb4e20",
      "latencyMs": 29,
      "detectedAfterSeconds": 18.029,
      "detail": "OK"
    },
    {
      "scenario": "robo-en-curso",
      "expectedAlert": "VEHICULO_EN_FUGA",
      "zoneId": 8,
      "minSeverity": "HIGH",
      "passed": true,
      "foundSeverity": "CRITICAL",
      "alertId": "58ef93b0-8b7a-319f-aa32-c181b5910686",
      "latencyMs": 19,
      "detectedAfterSeconds": 90.015,
      "detail": "OK"
    }
  ]
}
```
_Se muestran 2 de los 9 resultados._

### 7.8 Vigilante del nodo B

**GET** `http://IP_C:8080/api/watchdog`

```json
{
  "watchdog": {
    "node": "nodo-c",
    "enabled": true,
    "checkedAt": "2026-10-31T18:20:06.619724383Z",
    "nodeB": {
      "state": "OPERATIVO",
      "since": "2026-10-31T18:18:24.314297654Z",
      "summary": "Todos los servicios responden."
    },
    "targets": [
      {
        "name": "ingestion",
        "url": "http://192.0.2.2:18081/actuator/health",
        "state": "UP",
        "since": "2026-10-31T18:18:24.314297654Z",
        "lastCheck": "2026-10-31T18:20:06.619724383Z",
        "latencyMs": 15,
        "detail": "UP",
        "consecutiveFailures": 0
      },
      {
        "name": "correlation",
        "url": "http://192.0.2.2:8082/actuator/health",
        "state": "UP",
        "since": "2026-10-31T18:18:24.314297654Z",
        "lastCheck": "2026-10-31T18:20:06.619724383Z",
        "latencyMs": 19,
        "detail": "UP",
        "consecutiveFailures": 0
      }
    ],
    "kafka": {
      "state": "UP",
      "since": "2026-10-31T18:18:24.314297654Z",
      "brokers": 1,
      "detail": "1 broker(s)",
      "groups": [
        {
          "group": "ingestion",
          "state": "Stable",
          "members": 2,
          "consumers": [
            "ingestion-service-nodo-b",
            "ingestion-service-nodo-c"
          ],
          "lag": 0,
          "trend": "BAJANDO",
          "condition": "OK",
          "message": "al dia"
        },
        {
          "group": "correlation",
          "state": "Stable",
          "members": 2,
          "consumers": [
            "correlation-engine-nodo-b",
            "correlation-engine-nodo-c"
          ],
          "lag": 0,
          "trend": "BAJANDO",
          "condition": "OK",
          "message": "al dia"
        }
      ]
    },
    "transitions": [
      {
        "at": "2026-10-31T18:18:24.314297654Z",
        "target": "kafka",
        "from": "UNKNOWN",
        "to": "UP",
        "detail": "1 broker(s)",
        "previousDurationSeconds": null
      },
      {
        "at": "2026-10-31T18:18:24.314297654Z",
        "target": "grafana",
        "from": "UNKNOWN",
        "to": "UP",
        "detail": "UP",
        "previousDurationSeconds": null
      }
    ]
  },
  "spool": {
    "critical": {
      "pendientes": 0,
      "enVuelo": 0,
      "bytes": 0,
      "masAntiguoSegundos": null,
      "guardados": 11,
      "reenviados": 0,
      "vencidos": 0,
      "descartadosPorTope": 0,
      "rechazadosPorTope": 0,
      "apartados": 0,
      "lineasInvalidas": 0,
      "erroresDeDisco": 0,
      "ultimoError": null,
      "ultimoReenvio": "2026-10-31T18:20:04.323690182Z",
      "problemasRecientes": []
    },
    "telemetry": {
      "pendientes": 0,
      "enVuelo": 0,
      "bytes": 0,
      "masAntiguoSegundos": null,
      "guardados": 0,
      "reenviados": 0,
      "vencidos": 0,
      "descartadosPorTope": 0,
      "rechazadosPorTope": 0,
      "apartados": 0,
      "lineasInvalidas": 0,
      "erroresDeDisco": 0,
      "ultimoError": null,
      "ultimoReenvio": null,
      "problemasRecientes": []
    }
  },
  "publisher": {
    "kafkaAceptaEnvios": true,
    "ultimoEnvioConfirmado": "2026-10-31T18:20:11.105030273Z",
    "ultimaFalla": null,
    "ultimoError": null
  }
}
```
_Se muestran 2 de 5 servicios y 2 de 6 transiciones. Es lo que dibuja `vigilante.html`._

### 7.9 Spool del simulador durante una caída de Kafka

**GET** `http://IP_C:8080/api/simulator/spool`

```json
{
  "critical": {
    "pendientes": 1,
    "enVuelo": 1,
    "bytes": 428,
    "masAntiguoSegundos": 4,
    "guardados": 56,
    "reenviados": 0,
    "vencidos": 0,
    "descartadosPorTope": 0,
    "rechazadosPorTope": 0,
    "apartados": 0,
    "lineasInvalidas": 0,
    "erroresDeDisco": 0,
    "ultimoError": null,
    "ultimoReenvio": "2026-10-31T18:30:54.373779216Z",
    "problemasRecientes": []
  },
  "telemetry": {
    "pendientes": 0,
    "enVuelo": 0,
    "bytes": 0,
    "masAntiguoSegundos": null,
    "guardados": 0,
    "reenviados": 0,
    "vencidos": 0,
    "descartadosPorTope": 0,
    "rechazadosPorTope": 0,
    "apartados": 0,
    "lineasInvalidas": 0,
    "erroresDeDisco": 0,
    "ultimoError": null,
    "ultimoReenvio": null,
    "problemasRecientes": []
  }
}
```
_Un evento crítico espera en `critical.wal` (`pendientes: 1`). `POST /api/simulator/spool/resend` fuerza una ronda de reenvío y responde con este mismo formato._

## 8. Archivos de configuración en JSON

### 8.1 Servicios del nodo B que vigila Prometheus

**Archivo:** `deploy/nodo-b/targets/servicios-nodo-b.json` · en el repositorio

```json
[
  {
    "targets": [
      "ingestion-service:8081"
    ],
    "labels": {
      "servicio": "ingestion-service"
    }
  },
  {
    "targets": [
      "correlation-engine:8082"
    ],
    "labels": {
      "servicio": "correlation-engine"
    }
  },
  {
    "targets": [
      "alert-service:8083"
    ],
    "labels": {
      "servicio": "alert-service"
    }
  }
]
```

### 8.2 Generadores que vigila Prometheus (lo escribe el script)

**Archivo:** `deploy/nodo-b/targets/generadores.json` · lo genera `centinela.cmd iniciar` en el nodo B; no se sube al repositorio

```json
[
  {
    "targets": [
      "192.0.2.2:8080"
    ],
    "labels": {
      "servicio": "sensor-simulator"
    }
  },
  {
    "targets": [
      "192.0.2.2:8081"
    ],
    "labels": {
      "servicio": "ingestion-service"
    }
  },
  {
    "targets": [
      "192.0.2.2:9082"
    ],
    "labels": {
      "servicio": "correlation-engine"
    }
  }
]
```
_Con *workers*, también vigila la ingesta y la correlación de C/D._

### 8.3 Servicios que vigila Prometheus en el modo de una sola PC

**Archivo:** `infra/prometheus/targets/servicios.json` · en el repositorio

```json
[
  {
    "targets": [
      "sensor-simulator:8080"
    ],
    "labels": {
      "servicio": "sensor-simulator"
    }
  },
  {
    "targets": [
      "ingestion-service:8081"
    ],
    "labels": {
      "servicio": "ingestion-service"
    }
  },
  {
    "targets": [
      "correlation-engine:8082"
    ],
    "labels": {
      "servicio": "correlation-engine"
    }
  },
  {
    "targets": [
      "alert-service:8083"
    ],
    "labels": {
      "servicio": "alert-service"
    }
  }
]
```

### 8.4 Roles de esta PC

**Archivo:** `.centinela/local.json` · lo escribe `centinela.cmd configurar`; no se sube al repositorio

```json
{
  "Roles": [
    "A",
    "B",
    "C"
  ]
}
```
_En el equipo de pruebas corrían los tres nodos; en el laboratorio cada PC tiene el suyo, por ejemplo `["B"]`._

### 8.5 Tableros de Grafana

**Archivos:** `infra/grafana/dashboards/*.json` · en el repositorio; Grafana los carga al arrancar

| Archivo | Título | `uid` | Refresco | Paneles |
|---|---|---|---|---|
| `centro-de-monitoreo.json` | Centro de Monitoreo - Tiempo real | `centro-monitoreo` | 5s | 12 |
| `slo-y-salud.json` | SLOs y Salud del Sistema | `slo-salud` | 5s | 15 |
| `falsos-positivos.json` | Analisis de Falsos Positivos | `falsos-positivos` | 30s | 9 |

Ejemplo de un panel completo: la tarjeta **Críticas** del centro de monitoreo, que consulta TimescaleDB con SQL:

```json
{
  "id": 2,
  "type": "stat",
  "title": "Criticas",
  "description": "",
  "datasource": {
    "type": "grafana-postgresql-datasource",
    "uid": "timescaledb"
  },
  "gridPos": {
    "x": 4,
    "y": 0,
    "w": 4,
    "h": 4
  },
  "targets": [
    {
      "refId": "A",
      "datasource": {
        "type": "grafana-postgresql-datasource",
        "uid": "timescaledb"
      },
      "rawSql": "SELECT count(*) FROM v_alerts WHERE $__timeFilter(updated_at) AND severity = 'CRITICAL'",
      "format": "table",
      "rawQuery": true,
      "editorMode": "code"
    }
  ],
  "fieldConfig": {
    "defaults": {
      "unit": "none",
      "color": {
        "mode": "thresholds"
      },
      "thresholds": {
        "mode": "absolute",
        "steps": [
          {
            "color": "green",
            "value": null
          },
          {
            "color": "red",
            "value": 1
          }
        ]
      }
    },
    "overrides": []
  },
  "options": {
    "reduceOptions": {
      "calcs": [
        "lastNotNull"
      ],
      "fields": "",
      "values": false
    },
    "colorMode": "background",
    "graphMode": "none",
    "textMode": "value",
    "justifyMode": "center",
    "orientation": "auto"
  }
}
```

### 8.6 Mapa de las 12 zonas (GeoJSON)

**Archivo:** `infra/grafana/maps/zonas-guatemala.geojson` · capa local del mapa de Grafana (funciona sin internet)

```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "properties": {
        "name": "Zona 1"
      },
      "geometry": {
        "type": "Polygon",
        "coordinates": [
          [
            [
              -90.50653,
              14.65043
            ],
            [
              -90.52565,
              14.65145
            ],
            [
              -90.51591,
              14.63321
            ],
            [
              -90.50803,
              14.63178
            ],
            [
              -90.49907,
              14.63964
            ],
            [
              -90.50653,
              14.65043
            ]
          ]
        ]
      }
    }
  ]
}
```
_Se muestra 1 de 12 zonas. Las coordenadas van en el orden longitud, latitud, como pide GeoJSON._
