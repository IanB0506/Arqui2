package gt.umg.centinela.alerts.jobs;

import gt.umg.centinela.alerts.config.AlertServiceProperties;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.Instant;

/** Cierra como EXPIRED las alertas abiertas que nadie atendio en el tiempo maximo (2 h por defecto). */
@Component
public class AlertExpiryJob {

    private static final Logger log = LoggerFactory.getLogger(AlertExpiryJob.class);

    private final JdbcTemplate jdbc;
    private final AlertServiceProperties properties;

    public AlertExpiryJob(JdbcTemplate jdbc, AlertServiceProperties properties) {
        this.jdbc = jdbc;
        this.properties = properties;
    }

    @Scheduled(fixedDelay = 300_000, initialDelay = 60_000)
    @SchedulerLock(name = "alert-expiry", lockAtMostFor = "PT4M")
    public void expire() {
        int expired = jdbc.update("UPDATE alerts SET status = 'EXPIRED' WHERE status IN ('NEW', 'ESCALATED', 'UPDATED') AND updated_at < ?",
                Timestamp.from(Instant.now().minus(properties.alertExpiry())));
        if (expired > 0) {
            log.info("{} alertas sin atender marcadas como EXPIRED", expired);
        }
    }
}
