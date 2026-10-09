-- Agregado continuo: eventos por minuto, zona y tipo. TimescaleDB lo mantiene actualizado
-- automaticamente; Grafana lo consulta en lugar de recorrer millones de filas.
CREATE MATERIALIZED VIEW events_per_minute
WITH (timescaledb.continuous) AS
SELECT time_bucket(INTERVAL '1 minute', event_time) AS bucket,
       zone_id,
       sensor_type,
       count(*)                                 AS total,
       count(*) FILTER (WHERE anomalous)        AS anomalous
FROM sensor_events
GROUP BY bucket, zone_id, sensor_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('events_per_minute',
    start_offset      => INTERVAL '2 hours',
    end_offset        => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute');
