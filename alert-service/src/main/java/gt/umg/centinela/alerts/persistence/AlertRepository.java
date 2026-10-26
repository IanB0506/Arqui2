package gt.umg.centinela.alerts.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import gt.umg.centinela.common.json.Json;
import gt.umg.centinela.common.model.AlertEvent;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/** Acceso a las tablas {@code alerts} y {@code alert_revisions}. */
@Repository
public class AlertRepository {

    /**
     * Upsert: una revision nueva del mismo incidente actualiza la fila, pero solo si es mas reciente.
     * Si un operador ya valido la alerta, su decision (CONFIRMED / FALSE_POSITIVE) se respeta.
     */
    private static final String UPSERT = """
            INSERT INTO alerts (alert_id, revision, rule_id, alert_type, severity, status, zone_id, zone_name, latitude,
                longitude, confidence, title, description, created_at, updated_at, first_event_time, trigger_event_time,
                latency_ms, evidence, plate_masked, plate_hash, parent_alert_id)
            VALUES (:alertId, :revision, :ruleId, :alertType, :severity, :status, :zoneId, :zoneName, :latitude,
                :longitude, :confidence, :title, :description, :createdAt, :createdAt, :firstEventTime,
                :triggerEventTime, :latencyMs, CAST(:evidence AS jsonb), :plateMasked, :plateHash, :parentAlertId)
            ON CONFLICT (alert_id) DO UPDATE SET
                revision = EXCLUDED.revision,
                rule_id = EXCLUDED.rule_id,
                severity = EXCLUDED.severity,
                status = CASE WHEN alerts.status IN ('CONFIRMED', 'FALSE_POSITIVE') THEN alerts.status ELSE EXCLUDED.status END,
                confidence = EXCLUDED.confidence,
                description = EXCLUDED.description,
                updated_at = EXCLUDED.updated_at,
                first_event_time = LEAST(alerts.first_event_time, EXCLUDED.first_event_time),
                trigger_event_time = EXCLUDED.trigger_event_time,
                latency_ms = EXCLUDED.latency_ms,
                evidence = EXCLUDED.evidence,
                plate_masked = COALESCE(EXCLUDED.plate_masked, alerts.plate_masked),
                plate_hash = COALESCE(EXCLUDED.plate_hash, alerts.plate_hash)
            WHERE alerts.revision < EXCLUDED.revision
            """;

    private static final String INSERT_REVISION = """
            INSERT INTO alert_revisions (alert_id, revision, alert_type, severity, status, zone_id, confidence,
                detected_at, received_at, trigger_event_time, latency_ms)
            VALUES (:alertId, :revision, :alertType, :severity, :status, :zoneId, :confidence, :createdAt, :receivedAt,
                :triggerEventTime, :latencyMs)
            ON CONFLICT DO NOTHING
            """;

    private static final String SELECT = """
            SELECT alert_id, revision, rule_id, alert_type, severity, status, zone_id, zone_name, latitude, longitude,
                   confidence, title, description, created_at, updated_at, trigger_event_time, latency_ms, plate_masked,
                   evidence, parent_alert_id, validated_by, validated_at, resolution_notes
            FROM alerts
            """;

    private final NamedParameterJdbcTemplate jdbc;

    public AlertRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Guarda una revision de alerta. Devuelve la latencia extremo a extremo medida (ms). */
    @Transactional
    public long save(AlertEvent alert, Instant receivedAt) {
        long latency = Math.max(0, Duration.between(alert.triggerEventTime(), receivedAt).toMillis());
        MapSqlParameterSource p = new MapSqlParameterSource()
                .addValue("alertId", alert.alertId())
                .addValue("revision", alert.revision())
                .addValue("ruleId", alert.ruleId())
                .addValue("alertType", alert.alertType())
                .addValue("severity", alert.severity().name())
                .addValue("status", alert.status().name())
                .addValue("zoneId", alert.zoneId())
                .addValue("zoneName", alert.zoneName())
                .addValue("latitude", alert.latitude())
                .addValue("longitude", alert.longitude())
                .addValue("confidence", alert.confidence())
                .addValue("title", alert.title())
                .addValue("description", alert.description())
                .addValue("createdAt", Timestamp.from(alert.createdAt()))
                .addValue("receivedAt", Timestamp.from(receivedAt))
                .addValue("firstEventTime", alert.firstEventTime() == null ? null : Timestamp.from(alert.firstEventTime()))
                .addValue("triggerEventTime", Timestamp.from(alert.triggerEventTime()))
                .addValue("latencyMs", latency)
                .addValue("evidence", Json.write(alert.evidence()))
                .addValue("plateMasked", alert.plateMasked())
                .addValue("plateHash", alert.plateHash())
                .addValue("parentAlertId", alert.parentAlertId());
        jdbc.update(UPSERT, p);
        jdbc.update(INSERT_REVISION, p);
        return latency;
    }

    /** Alerta generada por el propio sistema (p. ej. incumplimiento de SLO). */
    public void saveSystemAlert(UUID id, String type, String severity, String title, String description, Instant at) {
        jdbc.update("""
                INSERT INTO alerts (alert_id, revision, rule_id, alert_type, severity, status, zone_id, zone_name,
                    confidence, title, description, created_at, updated_at, trigger_event_time, latency_ms, evidence)
                VALUES (:id, 1, 'SISTEMA', :type, :severity, 'NEW', 0, 'Sistema', 1.0, :title, :description, :at, :at,
                    :at, 0, '[]'::jsonb)
                """, new MapSqlParameterSource()
                .addValue("id", id).addValue("type", type).addValue("severity", severity)
                .addValue("title", title).addValue("description", description).addValue("at", Timestamp.from(at)));
    }

    public List<AlertView> find(Instant since, Integer zoneId, String type, String status, int limit) {
        StringBuilder sql = new StringBuilder(SELECT).append(" WHERE updated_at >= :since");
        MapSqlParameterSource p = new MapSqlParameterSource("since", Timestamp.from(since)).addValue("limit", limit);
        if (zoneId != null) {
            sql.append(" AND zone_id = :zoneId");
            p.addValue("zoneId", zoneId);
        }
        if (type != null && !type.isBlank()) {
            sql.append(" AND alert_type = :type");
            p.addValue("type", type);
        }
        if (status != null && !status.isBlank()) {
            sql.append(" AND status = :status");
            p.addValue("status", status);
        }
        sql.append(" ORDER BY updated_at DESC LIMIT :limit");
        return jdbc.query(sql.toString(), p, MAPPER);
    }

    public Optional<AlertView> findById(UUID id) {
        return jdbc.query(SELECT + " WHERE alert_id = :id", Map.of("id", id), MAPPER).stream().findFirst();
    }

    public boolean validate(UUID id, String resolution, String operator, String notes, Instant at) {
        return jdbc.update("""
                UPDATE alerts SET status = :resolution, validated_by = :operator, validated_at = :at,
                    resolution_notes = :notes
                WHERE alert_id = :id
                """, new MapSqlParameterSource()
                .addValue("id", id).addValue("resolution", resolution).addValue("operator", operator)
                .addValue("notes", notes).addValue("at", Timestamp.from(at))) == 1;
    }

    /** Resumen de la consola: conteos desde {@code since} y p95 de latencia desde {@code latencySince}. */
    public Map<String, Object> summary(Instant since, Instant latencySince) {
        return jdbc.queryForMap("""
                SELECT count(*)                                                   AS total,
                       count(*) FILTER (WHERE severity = 'CRITICAL')              AS critical,
                       count(*) FILTER (WHERE severity = 'HIGH')                  AS high,
                       count(*) FILTER (WHERE status IN ('NEW', 'ESCALATED', 'UPDATED')) AS open,
                       count(*) FILTER (WHERE alert_type = 'POSIBLE_FALSO_POSITIVO') AS possible_false_positives,
                       count(*) FILTER (WHERE status = 'FALSE_POSITIVE')          AS validated_false_positives,
                       count(*) FILTER (WHERE status = 'CONFIRMED')               AS confirmed,
                       (SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY r.latency_ms) FROM alert_revisions r
                        WHERE r.received_at >= :latencySince AND r.status IN ('NEW', 'ESCALATED')
                          AND r.severity IN ('HIGH', 'CRITICAL'))                 AS p95_latency_ms
                FROM alerts WHERE updated_at >= :since
                """, Map.of("since", Timestamp.from(since), "latencySince", Timestamp.from(latencySince)));
    }

    private static final RowMapper<AlertView> MAPPER = AlertRepository::map;

    private static AlertView map(ResultSet rs, int row) throws SQLException {
        try {
            String evidence = rs.getString("evidence");
            return new AlertView(
                    rs.getObject("alert_id", UUID.class),
                    rs.getInt("revision"),
                    rs.getString("rule_id"),
                    rs.getString("alert_type"),
                    rs.getString("severity"),
                    rs.getString("status"),
                    rs.getInt("zone_id"),
                    rs.getString("zone_name"),
                    (Double) rs.getObject("latitude"),
                    (Double) rs.getObject("longitude"),
                    (Double) rs.getObject("confidence"),
                    rs.getString("title"),
                    rs.getString("description"),
                    instant(rs, "created_at"),
                    instant(rs, "updated_at"),
                    instant(rs, "trigger_event_time"),
                    (Long) rs.getObject("latency_ms"),
                    rs.getString("plate_masked"),
                    evidence == null ? null : Json.MAPPER.readTree(evidence),
                    rs.getObject("parent_alert_id", UUID.class),
                    rs.getString("validated_by"),
                    instant(rs, "validated_at"),
                    rs.getString("resolution_notes"));
        } catch (JsonProcessingException e) {
            throw new SQLException("Evidencia con JSON invalido", e);
        }
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        Timestamp ts = rs.getTimestamp(column);
        return ts == null ? null : ts.toInstant();
    }
}
