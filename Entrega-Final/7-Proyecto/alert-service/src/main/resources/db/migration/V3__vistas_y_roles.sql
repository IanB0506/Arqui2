-- =============================================================================
-- Acceso de solo lectura para Grafana (privacidad por diseno, ADR-007):
-- Grafana nunca ve seudonimos de placas ni de ciudadanos, solo vistas filtradas.
-- =============================================================================
CREATE VIEW v_events AS
SELECT event_id, event_time, ingested_at, stored_at, sensor_type, sensor_id, zone_id, zone_name,
       latitude, longitude, subtype, value, confidence, anomalous, plate_masked, origin
FROM sensor_events;

CREATE VIEW v_alerts AS
SELECT alert_id, revision, rule_id, alert_type, severity, status, zone_id, zone_name, latitude, longitude,
       confidence, title, description, created_at, updated_at, first_event_time, trigger_event_time,
       latency_ms, plate_masked, parent_alert_id, validated_by IS NOT NULL AS validated, validated_at
FROM alerts;

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'grafana_reader') THEN
        CREATE ROLE grafana_reader LOGIN PASSWORD '${grafanaPassword}';
    END IF;
END
$$;

GRANT USAGE ON SCHEMA public TO grafana_reader;
GRANT SELECT ON v_events, v_alerts, events_per_minute, alert_revisions, slo_measurements, rule_quality
    TO grafana_reader;
