package gt.umg.centinela.alerts.persistence;

import gt.umg.centinela.common.json.Json;
import gt.umg.centinela.common.model.NormalizedEvent;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;

/** Escritura por lotes de eventos normalizados en la hypertable {@code sensor_events}. */
@Repository
public class EventRepository {

    private static final String INSERT = """
            INSERT INTO sensor_events (event_id, event_time, ingested_at, sensor_type, sensor_id, zone_id, zone_name,
                latitude, longitude, subtype, value, confidence, anomalous, plate_hash, plate_masked, source_hash,
                origin, attributes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)
            ON CONFLICT DO NOTHING
            """;

    private final JdbcTemplate jdbc;

    public EventRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Idempotente: si Kafka reentrega un lote, los eventos ya guardados se ignoran (ON CONFLICT). */
    public int saveAll(List<NormalizedEvent> events) {
        if (events.isEmpty()) {
            return 0;
        }
        int[][] result = jdbc.batchUpdate(INSERT, events, 500, (ps, e) -> {
            ps.setObject(1, e.eventId());
            ps.setTimestamp(2, Timestamp.from(e.eventTime()));
            ps.setTimestamp(3, Timestamp.from(e.ingestedAt()));
            ps.setString(4, e.sensorType().name());
            ps.setString(5, e.sensorId());
            ps.setInt(6, e.zoneId());
            ps.setString(7, e.zoneName());
            ps.setDouble(8, e.latitude());
            ps.setDouble(9, e.longitude());
            ps.setString(10, e.subtype());
            if (e.value() == null) {
                ps.setNull(11, Types.DOUBLE);
            } else {
                ps.setDouble(11, e.value());
            }
            ps.setDouble(12, e.confidence());
            ps.setBoolean(13, e.anomalous());
            ps.setString(14, e.plateHash());
            ps.setString(15, e.plateMasked());
            ps.setString(16, e.sourceHash());
            ps.setString(17, e.origin());
            ps.setString(18, e.attributes() == null ? null : Json.write(e.attributes()));
        });
        int total = 0;
        for (int[] batch : result) {
            for (int r : batch) {
                total += Math.max(r, 0);
            }
        }
        return total;
    }
}
