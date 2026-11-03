-- Privacidad (ADR-007): Grafana (usuario grafana_reader) no debe ver seudónimos. El id de un celular
-- es "APP-" + un trozo de su seudónimo HMAC, y permitiría seguir a un mismo dispositivo en los
-- tableros; en la vista se muestra solo "APP". Los postes, cámaras y sensores fijos conservan su id.
CREATE OR REPLACE VIEW v_events AS
SELECT event_id, event_time, ingested_at, stored_at, sensor_type,
       CASE WHEN sensor_id LIKE 'APP-%' THEN 'APP' ELSE sensor_id END AS sensor_id,
       zone_id, zone_name, latitude, longitude, subtype, value, confidence, anomalous, plate_masked, origin
FROM sensor_events;
