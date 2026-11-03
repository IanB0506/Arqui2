package gt.umg.centinela.alerts.jobs;

import gt.umg.centinela.alerts.config.AlertServiceProperties;
import gt.umg.centinela.alerts.persistence.AuditRepository;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;

@Component
public class PlateRetentionJob {
    private static final Logger log = LoggerFactory.getLogger(PlateRetentionJob.class);

    private final JdbcTemplate jdbc;
    private final AuditRepository audit;
    private final AlertServiceProperties properties;

    public PlateRetentionJob(JdbcTemplate jdbc, AuditRepository audit, AlertServiceProperties properties) {
        this.jdbc = jdbc;
        this.audit = audit;
        this.properties = properties;
    }

    @Scheduled(cron = "0 15 2 * * *", zone = "America/Guatemala")
    @SchedulerLock(name = "plate-retention", lockAtMostFor = "PT30M")
    public void purge() {
        Timestamp limit = Timestamp.from(Instant.now().minus(properties.plateRetention()));
        int events = jdbc.update("UPDATE sensor_events SET plate_hash = NULL WHERE plate_hash IS NOT NULL AND event_time < ?",
                limit);
        int alertRows = jdbc.update("""
                UPDATE alerts SET plate_hash = NULL, plate_masked = NULL
                WHERE plate_hash IS NOT NULL AND created_at < ? AND status <> 'CONFIRMED'
                """, limit);
        audit.record("sistema", "PURGA_SEUDONIMOS_PLACA", "sensor_events/alerts",
                Map.of("events", events, "alerts", alertRows, "olderThan", limit.toInstant().toString()));
        log.info("Retencion de placas: {} eventos y {} alertas anonimizados", events, alertRows);
    }
}
