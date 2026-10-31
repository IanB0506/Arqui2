# JSON principales de Centinela Urbano

## Crudos (entrada)

### 1. Botón de pánico desde la app · `sensor.panic.raw`

**Campos:** `event_id` · `timestamp` · `zona_geografica` (`nombre`, `latitud`, `longitud`) · `tipo_alerta` · `identificador_dispositivo` · `user_context` (`usuario_id`, `plataforma`, `version_app`)

**Cabeceras de Kafka:** `centinela-first-attempt`

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

### 2. Lectura de una cámara LPR · `sensor.lpr.raw`

**Campos:** `event_id` · `timestamp` · `placa_vehicular` · `ubicacion_sensor` (`sensor_id`, `zona`, `latitud`, `longitud`) · `velocidad_estimada` · `modelo_vehiculo` · `color_vehiculo`

**Cabeceras de Kafka:** `centinela-first-attempt`

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

### 3. Lectura de un sensor de velocidad · `sensor.speed.raw`

**Campos:** `event_id` · `timestamp` · `sensor_id` · `zona` · `velocidad_detectada` · `direccion` · `contravia` · `limite_velocidad`

**Cabeceras de Kafka:** `centinela-first-attempt`

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

### 4. Detección de un sensor acústico (disparo) · `sensor.acoustic.raw`

**Campos:** `event_id` · `timestamp` · `sensor_id` · `zona` · `tipo_sonido_detectado` · `nivel_decibeles` · `probabilidad_evento_critico`

**Cabeceras de Kafka:** `centinela-first-attempt`

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

### 5. Reporte ciudadano con texto libre · `sensor.citizen.raw`

**Campos:** `event_id` · `timestamp` · `tipo_evento` · `mensaje_descriptivo` · `ubicacion_aproximada` (`nombre`, `latitud`, `longitud`) · `origen` · `reportante_id`

**Cabeceras de Kafka:** `centinela-first-attempt`

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

## Normalizado

### 6. Evento normalizado (la lectura LPR del punto 2) · `events.normalized`

**Campos:** `eventId` · `sensorType` · `sensorId` · `zoneId` · `zoneName` · `latitude` · `longitude` · `eventTime` · `ingestedAt` · `subtype` · `value` · `confidence` · `anomalous` · `plateHash` · `plateMasked` · `sourceHash` · `origin` · `attributes` (`modelo`, `color`)

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

## Alerta

### 7. Alerta de robo en curso CRITICAL (tabla 2.4, con los puntos 2 y 3 como evidencia) · `alerts`

**Campos:** `alertId` · `revision` · `ruleId` · `alertType` · `severity` · `status` · `zoneId` · `zoneName` · `latitude` · `longitude` · `confidence` · `title` · `description` · `createdAt` · `firstEventTime` · `triggerEventTime` · `evidence[]` (`eventId`, `sensorType`, `subtype`, `sensorId`, `eventTime`, `weight`, `summary`, `detail`) · `plateMasked` · `plateHash` · `parentAlertId`

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
