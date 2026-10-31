# JSON del proyecto Centinela Urbano

## 1. Mensajes crudos de los sensores (Kafka)

### 1.1 Botón de pánico desde la app (enviado por el gateway) · `sensor.panic.raw`

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

### 1.2 Botón de pánico de la app generado por el simulador · `sensor.panic.raw`

**Campos:** `event_id` · `timestamp` · `zona_geografica` (`nombre`, `latitud`, `longitud`) · `tipo_alerta` · `identificador_dispositivo` · `user_context` (`usuario_id`, `plataforma`, `version_app`)

**Cabeceras de Kafka:** `centinela-first-attempt`

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

### 1.3 Botón de pánico físico (poste) · `sensor.panic.raw`

**Campos:** `event_id` · `timestamp` · `zona_geografica` (`nombre`, `latitud`, `longitud`) · `tipo_alerta` · `identificador_dispositivo` · `user_context`

**Cabeceras de Kafka:** `centinela-first-attempt`

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

### 1.4 Lectura de una cámara LPR · `sensor.lpr.raw`

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

### 1.5 Lectura de un sensor de velocidad · `sensor.speed.raw`

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

### 1.6 Detección de un sensor acústico (disparo) · `sensor.acoustic.raw`

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

### 1.7 Reporte ciudadano con texto libre · `sensor.citizen.raw`

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

### 1.8 Pánico reenviado desde el spool · `sensor.panic.raw`

**Campos:** `event_id` · `timestamp` · `zona_geografica` (`nombre`, `latitud`, `longitud`) · `tipo_alerta` · `identificador_dispositivo` · `user_context` (`usuario_id`, `plataforma`, `version_app`)

**Cabeceras de Kafka:** `centinela-first-attempt` · `centinela-spooled`

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

## 2. Eventos normalizados (Kafka, `events.normalized`)

### 2.1 Pánico de la app normalizado

**Campos:** `eventId` · `sensorType` · `sensorId` · `zoneId` · `zoneName` · `latitude` · `longitude` · `eventTime` · `ingestedAt` · `subtype` · `value` · `confidence` · `anomalous` · `plateHash` · `plateMasked` · `sourceHash` · `origin` · `attributes` (`plataforma`, `version_app`)

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

### 2.2 Pánico físico normalizado

**Campos:** `eventId` · `sensorType` · `sensorId` · `zoneId` · `zoneName` · `latitude` · `longitude` · `eventTime` · `ingestedAt` · `subtype` · `value` · `confidence` · `anomalous` · `plateHash` · `plateMasked` · `sourceHash` · `origin` · `attributes`

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

### 2.3 Lectura LPR normalizada

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

### 2.4 Lectura de velocidad normalizada

**Campos:** `eventId` · `sensorType` · `sensorId` · `zoneId` · `zoneName` · `latitude` · `longitude` · `eventTime` · `ingestedAt` · `subtype` · `value` · `confidence` · `anomalous` · `plateHash` · `plateMasked` · `sourceHash` · `origin` · `attributes` (`direccion`, `limite`)

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

### 2.5 Detección acústica normalizada

**Campos:** `eventId` · `sensorType` · `sensorId` · `zoneId` · `zoneName` · `latitude` · `longitude` · `eventTime` · `ingestedAt` · `subtype` · `value` · `confidence` · `anomalous` · `plateHash` · `plateMasked` · `sourceHash` · `origin` · `attributes`

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

### 2.6 Reporte ciudadano normalizado

**Campos:** `eventId` · `sensorType` · `sensorId` · `zoneId` · `zoneName` · `latitude` · `longitude` · `eventTime` · `ingestedAt` · `subtype` · `value` · `confidence` · `anomalous` · `plateHash` · `plateMasked` · `sourceHash` · `origin` · `attributes` (`mensaje`)

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

### 2.7 Evento normalizado que llegó desde un spool

**Campos:** `eventId` · `sensorType` · `sensorId` · `zoneId` · `zoneName` · `latitude` · `longitude` · `eventTime` · `ingestedAt` · `subtype` · `value` · `confidence` · `anomalous` · `plateHash` · `plateMasked` · `sourceHash` · `origin` · `attributes` (`plataforma`, `version_app`, `reenviado_spool`, `retraso_origen_s`)

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

## 3. Alertas (Kafka, `alerts`)

### 3.1 Robo en curso CRITICAL (fila 1 de la tabla 2.4)

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

### 3.2 Robo que escala de HIGH a CRITICAL (revisión 2)

**Campos:** `alertId` · `revision` · `ruleId` · `alertType` · `severity` · `status` · `zoneId` · `zoneName` · `latitude` · `longitude` · `confidence` · `title` · `description` · `createdAt` · `firstEventTime` · `triggerEventTime` · `evidence[]` (`eventId`, `sensorType`, `subtype`, `sensorId`, `eventTime`, `weight`, `summary`, `detail`) · `plateMasked` · `plateHash` · `parentAlertId`

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

### 3.3 Vehículo en fuga

**Campos:** `alertId` · `revision` · `ruleId` · `alertType` · `severity` · `status` · `zoneId` · `zoneName` · `latitude` · `longitude` · `confidence` · `title` · `description` · `createdAt` · `firstEventTime` · `triggerEventTime` · `evidence[]` (`eventId`, `sensorType`, `subtype`, `sensorId`, `eventTime`, `weight`, `summary`, `detail`) · `plateMasked` · `plateHash` · `parentAlertId`

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

### 3.4 Posible falso positivo

**Campos:** `alertId` · `revision` · `ruleId` · `alertType` · `severity` · `status` · `zoneId` · `zoneName` · `latitude` · `longitude` · `confidence` · `title` · `description` · `createdAt` · `firstEventTime` · `triggerEventTime` · `evidence[]` (`eventId`, `sensorType`, `subtype`, `sensorId`, `eventTime`, `weight`, `summary`, `detail`) · `plateMasked` · `plateHash` · `parentAlertId`

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

## 4. Cola de mensajes muertos (Kafka, `events.dlq`)

### 4.1 JSON sin los campos obligatorios

**Campos:** `tipo_alerta` · `zona_geografica` (`nombre`)

**Cabeceras de Kafka:** `kafka_dlt-exception-fqcn` · `kafka_dlt-exception-cause-fqcn` · `kafka_dlt-exception-message` · `kafka_dlt-exception-stacktrace` · `kafka_dlt-original-topic` · `kafka_dlt-original-partition` · `kafka_dlt-original-offset` · `kafka_dlt-original-timestamp` · `kafka_dlt-original-timestamp-type` · `kafka_dlt-original-consumer-group` · `centinela-error-class` · `centinela-service`

```json
{
  "tipo_alerta": "ROBO",
  "zona_geografica": {
    "nombre": "Zona 3"
  }
}
```

### 4.2 Pánico con una zona que no existe

**Campos:** `event_id` · `timestamp` · `zona_geografica` (`nombre`, `latitud`, `longitud`) · `tipo_alerta` · `identificador_dispositivo` · `user_context`

**Cabeceras de Kafka:** `centinela-first-attempt` · `kafka_dlt-exception-fqcn` · `kafka_dlt-exception-cause-fqcn` · `kafka_dlt-exception-message` · `kafka_dlt-exception-stacktrace` · `kafka_dlt-original-topic` · `kafka_dlt-original-partition` · `kafka_dlt-original-offset` · `kafka_dlt-original-timestamp` · `kafka_dlt-original-timestamp-type` · `kafka_dlt-original-consumer-group` · `centinela-error-class` · `centinela-service`

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

## 5. Gateway de la app (HTTP, nodo B, puerto 8081)

### 5.1 Pánico enviado por la app · `POST /api/v1/panic`

**Campos:** `event_id` · `timestamp` · `zona_geografica` (`nombre`, `latitud`, `longitud`) · `tipo_alerta` · `identificador_dispositivo` · `user_context` (`usuario_id`, `plataforma`, `version_app`)

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

### 5.2 Respuesta 202: recibido por Kafka

**Campos:** `event_id` · `estado`

```json
{
  "event_id": "edf5c3f8-0bae-4d44-a826-61b43c73d95a",
  "estado": "RECIBIDO"
}
```

### 5.3 Pánico enviado con Kafka caído · `POST /api/v1/panic`

**Campos:** `event_id` · `timestamp` · `zona_geografica` (`nombre`, `latitud`, `longitud`) · `tipo_alerta` · `identificador_dispositivo` · `user_context` (`usuario_id`, `plataforma`, `version_app`)

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

### 5.4 Respuesta 202: guardado en el spool (EN_COLA)

**Campos:** `event_id` · `estado`

```json
{
  "event_id": "0ced83f1-9fee-419c-82df-e48c5dbd3833",
  "estado": "EN_COLA"
}
```

### 5.5 Respuesta 503: sin Kafka ni disco (formato del código)

**Campos:** `event_id` · `estado` · `error`

```json
{
  "event_id": "<el event_id del pánico>",
  "estado": "NO_DISPONIBLE",
  "error": "No se pudo guardar el evento; reintente con el mismo event_id"
}
```

### 5.6 Respuesta 400: falta un campo obligatorio

**Campos:** `error`

```json
{
  "error": "Contrato invalido de PanicButtonEvent: deviceId must not be blank"
}
```

### 5.7 Respuesta 400: valor no permitido (formato de Spring Boot)

**Campos:** `timestamp` · `status` · `error` · `path`

```json
{
  "timestamp": "2026-10-31T18:19:35.859+00:00",
  "status": 400,
  "error": "Bad Request",
  "path": "/api/v1/panic"
}
```

### 5.8 Reporte ciudadano enviado por la app · `POST /api/v1/citizen-reports`

**Campos:** `event_id` · `timestamp` · `tipo_evento` · `mensaje_descriptivo` · `ubicacion_aproximada` (`latitud`, `longitud`) · `origen` · `reportante_id`

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

### 5.9 Respuesta 202 al reporte ciudadano

**Campos:** `event_id` · `estado`

```json
{
  "event_id": "a8cbf043-aec1-4c7e-b00a-e2fae12e2f2b",
  "estado": "RECIBIDO"
}
```

### 5.10 Estado del gateway en operación normal · `GET /api/v1/gateway/status`

**Campos:** `kafkaDisponible` · `spoolPendientes` · `enVuelo` · `reenviados` · `apartados` · `rechazadosPorTope` · `lineasInvalidas` · `ultimoError`

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

### 5.11 Estado del gateway con Kafka caído · `GET /api/v1/gateway/status`

**Campos:** `kafkaDisponible` · `spoolPendientes` · `enVuelo` · `reenviados` · `apartados` · `rechazadosPorTope` · `lineasInvalidas` · `ultimoError`

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

### 5.12 Estado del gateway después de recuperar Kafka · `GET /api/v1/gateway/status`

**Campos:** `kafkaDisponible` · `spoolPendientes` · `enVuelo` · `reenviados` · `apartados` · `rechazadosPorTope` · `lineasInvalidas` · `ultimoError`

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

## 6. API de la consola: alert-service (HTTP, nodo B, puerto 8083)

### 6.1 Lista de alertas (1 de 19) · `GET /api/alerts`

**Campos:** Lista; cada elemento: `alertId` · `revision` · `ruleId` · `alertType` · `severity` · `status` · `zoneId` · `zoneName` · `latitude` · `longitude` · `confidence` · `title` · `description` · `createdAt` · `updatedAt` · `triggerEventTime` · `latencyMs` · `plateMasked` · `evidence[]` (`detail`, `weight`, `eventId`, `subtype`, `summary`, `sensorId`, `eventTime`, `sensorType`) · `parentAlertId` · `validatedBy` · `validatedAt` · `resolutionNotes`

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

### 6.2 Detalle de una alerta · `GET /api/alerts/{id}`

**Campos:** `alertId` · `revision` · `ruleId` · `alertType` · `severity` · `status` · `zoneId` · `zoneName` · `latitude` · `longitude` · `confidence` · `title` · `description` · `createdAt` · `updatedAt` · `triggerEventTime` · `latencyMs` · `plateMasked` · `evidence[]` (`detail`, `weight`, `eventId`, `subtype`, `summary`, `sensorId`, `eventTime`, `sensorType`) · `parentAlertId` · `validatedBy` · `validatedAt` · `resolutionNotes`

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

### 6.3 Evento original de una evidencia · `GET /api/events/{id}?at=`

**Campos:** `event_id` · `event_time` · `ingested_at` · `stored_at` · `sensor_type` · `sensor_id` · `zone_id` · `zone_name` · `latitude` · `longitude` · `subtype` · `value` · `confidence` · `anomalous` · `plate_masked` · `origin` · `attributes` (`color`, `modelo`)

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

### 6.4 Validación del operador · `PATCH /api/alerts/{id}/validation`

**Campos:** `resolution` · `notes`

```json
{
  "resolution": "FALSE_POSITIVE",
  "notes": "Se llamo al usuario: presiono el boton por error"
}
```

### 6.5 Alerta después de validarla (respuesta)

**Campos:** `alertId` · `revision` · `ruleId` · `alertType` · `severity` · `status` · `zoneId` · `zoneName` · `latitude` · `longitude` · `confidence` · `title` · `description` · `createdAt` · `updatedAt` · `triggerEventTime` · `latencyMs` · `plateMasked` · `evidence[]` (`detail`, `weight`, `eventId`, `subtype`, `summary`, `sensorId`, `eventTime`, `sensorType`) · `parentAlertId` · `validatedBy` · `validatedAt` · `resolutionNotes`

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

### 6.6 Alerta recibida en vivo (evento SSE `alert`) · `GET /api/alerts/stream`

**Campos:** `alertId` · `revision` · `ruleId` · `alertType` · `severity` · `status` · `zoneId` · `zoneName` · `latitude` · `longitude` · `confidence` · `title` · `description` · `createdAt` · `updatedAt` · `triggerEventTime` · `latencyMs` · `plateMasked` · `evidence[]` (`detail`, `weight`, `eventId`, `subtype`, `summary`, `sensorId`, `eventTime`, `sensorType`) · `parentAlertId` · `validatedBy` · `validatedAt` · `resolutionNotes`

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

### 6.7 Resumen para la consola · `GET /api/stats/summary`

**Campos:** `total` · `critical` · `high` · `open` · `possible_false_positives` · `validated_false_positives` · `confirmed` · `p95_latency_ms` · `windowMinutes` · `latencyWindowMinutes` · `connectedConsoles`

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

### 6.8 Catálogo de zonas (2 de 12) · `GET /api/zones`

**Campos:** Lista; cada elemento: `id` · `name` · `latitude` · `longitude` · `row` · `col`

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

### 6.9 Configuración de la consola · `GET /api/console/config`

**Campos:** `grafanaPort`

```json
{
  "grafanaPort": 3000
}
```

### 6.10 Lista de placas robadas (2 de 6) · `GET /api/watchlist`

**Campos:** Lista; cada elemento: `plate_masked` · `reason` · `added_at` · `added_by`

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

### 6.11 Agregar una placa robada · `POST /api/watchlist`

**Campos:** `plate` · `reason`

```json
{
  "plate": "P999ZZZ",
  "reason": "Denuncia de robo 2026-1031"
}
```

### 6.12 Respuesta 201 al agregar la placa

**Campos:** `added` · `plate_masked`

```json
{
  "added": true,
  "plate_masked": "P99***"
}
```

### 6.13 Resumen de la DLQ (2 de los 3 últimos) · `GET /api/admin/dlq`

**Campos:** `total` · `porOrigenClaseYServicio` (mapa: «topic / clase / servicio» → cantidad) · `ultimos[]` (`hora`, `topicOriginal`, `clase`, `servicio`, `reprocesos`, `causa`)

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

### 6.14 Reprocesar la DLQ · `POST /api/admin/dlq/reprocess`

**Campos:** `reprocessed` · `skipped` · `read` · `incluirPermanentes`

```json
{
  "reprocessed": 0,
  "skipped": 0,
  "read": 0,
  "incluirPermanentes": false
}
```

### 6.15 Ejecutar una tarea periódica · `POST /api/admin/jobs/rule-quality`

**Campos:** `job` · `executedAt`

```json
{
  "job": "rule-quality",
  "executedAt": "2026-10-31T18:28:38.198165736Z"
}
```

### 6.16 Limpiar el estado de correlación de una zona · `POST /api/admin/correlation/reset?zonas=1`

**Campos:** `executedAt` · `detail` (mapa: patrón de llaves de Redis → llaves borradas) · `deletedKeys`

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

### 6.17 Informe de eventos críticos perdidos · `POST /api/admin/slo/lost-critical-events`

**Campos:** `node` · `lost`

```json
{
  "node": "nodo-c",
  "lost": 1
}
```

### 6.18 Respuesta al informe de eventos críticos perdidos

**Campos:** `registrado` · `perdidos`

```json
{
  "registrado": true,
  "perdidos": 1
}
```

### 6.19 Salud de un servicio · `GET /actuator/health`

**Campos:** `status` · `groups[]` · `components` (`diskSpace` (`status`), `livenessState` (`status`), `ping` (`status`), `readinessState` (`status`), `redis` (`status`), `ssl` (`status`))

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

## 7. API del simulador (HTTP, nodos C/D, puerto 8080)

### 7.1 Estado del simulador · `GET /api/simulator/status`

**Campos:** `node` · `zones[]` · `baselineEnabled` · `bursting` · `multiplier` · `expectedEventsPerSecond` · `ratesPerZone` (`PANIC`, `LPR`, `SPEED`, `ACOUSTIC`, `CITIZEN_REPORT`) · `spoolPending` · `spoolTelemetryPending` · `nodeB` · `quietZones` (mapa: zona → hora hasta la que queda aislada)

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

### 7.2 Lista de escenarios (2 de 10) · `GET /api/scenarios`

**Campos:** Lista; cada elemento: `id` · `title` · `description` · `defaultZone` · `includeInValidation` · `fixedZone`

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

### 7.3 Lanzar un escenario · `POST /api/scenarios/tabla-2-4`

**Campos:** `executionId` · `scenarioId` · `title` · `zoneId` · `startedAt` · `steps[]` (`at`, `description`) · `expected[]` (`alertType`, `zoneId`, `withinSeconds`, `minSeverity`) · `quietZones[]` · `quietUntil` · `quietSeconds` · `isolation`

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

### 7.4 Pico de carga · `POST /api/simulator/burst`

**Campos:** `node` · `zones[]` · `baselineEnabled` · `bursting` · `multiplier` · `expectedEventsPerSecond` · `ratesPerZone` (`PANIC`, `LPR`, `SPEED`, `ACOUSTIC`, `CITIZEN_REPORT`) · `spoolPending` · `spoolTelemetryPending` · `nodeB` · `quietZones` (mapa: zona → hora hasta la que queda aislada)

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

### 7.5 Replay de un CSV · `POST /api/replay`

**Campos:** `source` · `events` · `durationSeconds` · `startedAt`

```json
{
  "source": "noche-de-viernes.csv",
  "events": 15,
  "durationSeconds": 26.487867283972967,
  "startedAt": "2026-10-31T18:28:47.262065072Z"
}
```

### 7.6 Iniciar la validación automática · `POST /api/validation`

**Campos:** `id` · `status` · `startedAt` · `finishedAt` · `passed` · `total` · `stateReset` · `results[]`

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

### 7.7 Resultado de la validación (2 de 9 resultados) · `GET /api/validation/latest`

**Campos:** `id` · `status` · `startedAt` · `finishedAt` · `passed` · `total` · `stateReset` · `results[]` (`scenario`, `expectedAlert`, `zoneId`, `minSeverity`, `passed`, `foundSeverity`, `alertId`, `latencyMs`, `detectedAfterSeconds`, `detail`)

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

### 7.8 Vigilante del nodo B (2 de 5 servicios y 2 de 6 transiciones) · `GET /api/watchdog`

**Campos:** `watchdog` (`node`, `enabled`, `checkedAt`, `nodeB` (`state`, `since`, `summary`), `targets[]` (`name`, `url`, `state`, `since`, `lastCheck`, `latencyMs`, `detail`, `consecutiveFailures`), `kafka` (`state`, `since`, `brokers`, `detail`, `groups[]` (…)), `transitions[]` (`at`, `target`, `from`, `to`, `detail`, `previousDurationSeconds`)) · `spool` (`critical` (`pendientes`, `enVuelo`, `bytes`, `masAntiguoSegundos`, `guardados`, `reenviados`, `vencidos`, `descartadosPorTope`, `rechazadosPorTope`, `apartados`, `lineasInvalidas`, `erroresDeDisco`, `ultimoError`, `ultimoReenvio`, `problemasRecientes[]`), `telemetry` (`pendientes`, `enVuelo`, `bytes`, `masAntiguoSegundos`, `guardados`, `reenviados`, `vencidos`, `descartadosPorTope`, `rechazadosPorTope`, `apartados`, `lineasInvalidas`, `erroresDeDisco`, `ultimoError`, `ultimoReenvio`, `problemasRecientes[]`)) · `publisher` (`kafkaAceptaEnvios`, `ultimoEnvioConfirmado`, `ultimaFalla`, `ultimoError`)

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

### 7.9 Spool del simulador durante una caída de Kafka · `GET /api/simulator/spool`

**Campos:** `critical` (`pendientes`, `enVuelo`, `bytes`, `masAntiguoSegundos`, `guardados`, `reenviados`, `vencidos`, `descartadosPorTope`, `rechazadosPorTope`, `apartados`, `lineasInvalidas`, `erroresDeDisco`, `ultimoError`, `ultimoReenvio`, `problemasRecientes[]`) · `telemetry` (`pendientes`, `enVuelo`, `bytes`, `masAntiguoSegundos`, `guardados`, `reenviados`, `vencidos`, `descartadosPorTope`, `rechazadosPorTope`, `apartados`, `lineasInvalidas`, `erroresDeDisco`, `ultimoError`, `ultimoReenvio`, `problemasRecientes[]`)

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

## 8. Archivos de configuración en JSON

### 8.1 Servicios del nodo B para Prometheus · `deploy/nodo-b/targets/servicios-nodo-b.json`

**Campos:** Lista; cada elemento: `targets[]` · `labels` (`servicio`)

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

### 8.2 Generadores para Prometheus · `deploy/nodo-b/targets/generadores.json`

**Campos:** Lista; cada elemento: `targets[]` · `labels` (`servicio`)

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

### 8.3 Servicios para Prometheus en una sola PC · `infra/prometheus/targets/servicios.json`

**Campos:** Lista; cada elemento: `targets[]` · `labels` (`servicio`)

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

### 8.4 Roles de esta PC · `.centinela/local.json`

**Campos:** `Roles[]`

```json
{
  "Roles": [
    "A",
    "B",
    "C"
  ]
}
```

### 8.5 Panel «Críticas» de Grafana · `infra/grafana/dashboards/centro-de-monitoreo.json`

**Campos:** `id` · `type` · `title` · `description` · `datasource` (`type`, `uid`) · `gridPos` (`x`, `y`, `w`, `h`) · `targets[]` (`refId`, `datasource` (…), `rawSql`, `format`, `rawQuery`, `editorMode`) · `fieldConfig` (`defaults` (…), `overrides[]`) · `options` (`reduceOptions` (…), `colorMode`, `graphMode`, `textMode`, `justifyMode`, `orientation`)

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

### 8.6 Mapa de las zonas (1 de 12) · `infra/grafana/maps/zonas-guatemala.geojson`

**Campos:** `type` · `features[]` (`type`, `properties` (`name`), `geometry` (`type`, `coordinates[]`))

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
