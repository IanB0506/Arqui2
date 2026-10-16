-- =============================================================================
-- Centinela Urbano - esquema base (TimescaleDB = PostgreSQL + series de tiempo)
-- Ver ADR-003 (persistencia) y ADR-007 (seguridad y privacidad)
-- =============================================================================
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Eventos normalizados de todos los sensores. Es la tabla de mayor volumen: hypertable
-- particionada por dia, comprimida a los 45 dias y eliminada a los 90 dias.
CREATE TABLE sensor_events (
    event_id      UUID             NOT NULL,
    event_time    TIMESTAMPTZ      NOT NULL,
    ingested_at   TIMESTAMPTZ      NOT NULL,
    stored_at     TIMESTAMPTZ      NOT NULL DEFAULT now(),
    sensor_type   TEXT             NOT NULL,
    sensor_id     TEXT             NOT NULL,
    zone_id       SMALLINT         NOT NULL,
    zone_name     TEXT             NOT NULL,
    latitude      DOUBLE PRECISION NOT NULL,
    longitude     DOUBLE PRECISION NOT NULL,
    subtype       TEXT,
    value         DOUBLE PRECISION,
    confidence    DOUBLE PRECISION,
    anomalous     BOOLEAN          NOT NULL,
    plate_hash    TEXT,             -- seudonimo HMAC; la placa en claro NUNCA se almacena
    plate_masked  TEXT,             -- "XYZ***" para mostrar a operadores
    source_hash   TEXT,             -- seudonimo del ciudadano
    origin        TEXT,
    attributes    JSONB,
    PRIMARY KEY (event_id, event_time)
);
SELECT create_hypertable('sensor_events', 'event_time', chunk_time_interval => INTERVAL '1 day');
CREATE INDEX idx_events_zone_time ON sensor_events (zone_id, event_time DESC);
CREATE INDEX idx_events_type_time ON sensor_events (sensor_type, event_time DESC);
CREATE INDEX idx_events_plate ON sensor_events (plate_hash, event_time DESC) WHERE plate_hash IS NOT NULL;

ALTER TABLE sensor_events SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'sensor_type, zone_id',
    timescaledb.compress_orderby = 'event_time DESC, event_id'
);
SELECT add_compression_policy('sensor_events', INTERVAL '45 days');
SELECT add_retention_policy('sensor_events', INTERVAL '90 days');

-- Alertas / incidentes (estado actual). Volumen bajo: tabla relacional normal.
CREATE TABLE alerts (
    alert_id           UUID PRIMARY KEY,
    revision           INT              NOT NULL,
    rule_id            TEXT             NOT NULL,
    alert_type         TEXT             NOT NULL,
    severity           TEXT             NOT NULL,
    status             TEXT             NOT NULL,
    zone_id            SMALLINT         NOT NULL,
    zone_name          TEXT             NOT NULL,
    latitude           DOUBLE PRECISION,
    longitude          DOUBLE PRECISION,
    confidence         DOUBLE PRECISION,
    title              TEXT             NOT NULL,
    description        TEXT,
    created_at         TIMESTAMPTZ      NOT NULL,   -- primera deteccion
    updated_at         TIMESTAMPTZ      NOT NULL,   -- ultima revision recibida
    first_event_time   TIMESTAMPTZ,
    trigger_event_time TIMESTAMPTZ,
    latency_ms         BIGINT,                      -- evento disparador -> alerta persistida (SLO)
    evidence           JSONB,
    plate_masked       TEXT,
    plate_hash         TEXT,
    parent_alert_id    UUID,
    validated_by       TEXT,
    validated_at       TIMESTAMPTZ,
    resolution_notes   TEXT
);
CREATE INDEX idx_alerts_created ON alerts (created_at DESC);
CREATE INDEX idx_alerts_zone ON alerts (zone_id, created_at DESC);
CREATE INDEX idx_alerts_status ON alerts (status);

-- Historial de revisiones: cada deteccion o escalamiento queda registrado (trazabilidad y SLO).
CREATE TABLE alert_revisions (
    alert_id           UUID        NOT NULL,
    revision           INT         NOT NULL,
    alert_type         TEXT        NOT NULL,
    severity           TEXT        NOT NULL,
    status             TEXT        NOT NULL,
    zone_id            SMALLINT    NOT NULL,
    confidence         DOUBLE PRECISION,
    detected_at        TIMESTAMPTZ NOT NULL,       -- cuando el motor genero la revision
    received_at        TIMESTAMPTZ NOT NULL,       -- cuando quedo persistida
    trigger_event_time TIMESTAMPTZ NOT NULL,
    latency_ms         BIGINT      NOT NULL,
    PRIMARY KEY (alert_id, revision)
);
CREATE INDEX idx_revisions_received ON alert_revisions (received_at DESC);

-- Bitacora de auditoria: quien valido alertas, quien modifico la lista de placas, purgas de datos.
CREATE TABLE audit_log (
    id      BIGSERIAL PRIMARY KEY,
    at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    actor   TEXT        NOT NULL,
    action  TEXT        NOT NULL,
    target  TEXT,
    detail  JSONB
);

-- Lista de vehiculos con reporte de robo: solo se guarda el seudonimo.
CREATE TABLE plate_watchlist (
    plate_hash   TEXT PRIMARY KEY,
    plate_masked TEXT        NOT NULL,
    reason       TEXT        NOT NULL,
    added_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    added_by     TEXT        NOT NULL
);

-- Mediciones periodicas de SLO.
CREATE TABLE slo_measurements (
    measured_at        TIMESTAMPTZ PRIMARY KEY,
    window_seconds     INT              NOT NULL,
    alerts             INT              NOT NULL,
    p50_ms             DOUBLE PRECISION,
    p95_ms             DOUBLE PRECISION,
    p99_ms             DOUBLE PRECISION,
    max_ms             DOUBLE PRECISION,
    within_slo_ratio   DOUBLE PRECISION,
    freshness_p95_ms   DOUBLE PRECISION,
    breached           BOOLEAN          NOT NULL
);

-- Calidad de reglas por zona calculada a partir de las validaciones de los operadores.
CREATE TABLE rule_quality (
    rule_id              TEXT             NOT NULL,
    zone_id              SMALLINT         NOT NULL,
    computed_at          TIMESTAMPTZ      NOT NULL,
    validated            INT              NOT NULL,
    confirmed            INT              NOT NULL,
    false_positives      INT              NOT NULL,
    missed               INT              NOT NULL,
    precision            DOUBLE PRECISION,
    threshold_adjustment DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (rule_id, zone_id)
);

-- Bloqueos de ShedLock: garantiza que cada tarea periodica corra en una sola instancia.
CREATE TABLE shedlock (
    name       VARCHAR(64)  PRIMARY KEY,
    lock_until TIMESTAMP    NOT NULL,
    locked_at  TIMESTAMP    NOT NULL,
    locked_by  VARCHAR(255) NOT NULL
);
