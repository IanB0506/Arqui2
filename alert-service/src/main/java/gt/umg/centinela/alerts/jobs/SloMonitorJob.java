package gt.umg.centinela.alerts.jobs;

import gt.umg.centinela.alerts.config.AlertServiceProperties;
import gt.umg.centinela.alerts.persistence.AlertRepository;
import io.micrometer.core.instrument.MeterRegistry;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Monitor de SLO (ADR-008). Cada 30 s mide sobre los ultimos 5 minutos:
 * <ul>
 *   <li>SLO-1 latencia: p95 de (evento disparador -> alerta persistida) &lt; 2 s, para revisiones HIGH/CRITICAL
 *       que el operador debe conocer de inmediato: primer aviso (NEW) y agravamientos (ESCALATED).</li>
 *   <li>SLO-3 frescura: p95 de (evento -> fila guardada en TimescaleDB) &lt; 10 s.</li>
 * </ul>
 * Si se incumple, no solo se pinta en rojo en Grafana: el sistema actua.
 * <ol>
 *   <li>Activa la bandera {@code system:degraded} en Redis: la ingesta empieza a muestrear lecturas de
 *       velocidad normales (el mayor volumen) para liberar capacidad a los eventos criticos.</li>
 *   <li>Genera una alerta de sistema SLO_INCUMPLIDO visible en la consola del operador.</li>
 *   <li>Registra la medicion para el informe de cumplimiento.</li>
 * </ol>
 */
@Component
public class SloMonitorJob {

    private static final Logger log = LoggerFactory.getLogger(SloMonitorJob.class);
    static final String DEGRADED_KEY = "system:degraded";

    private final NamedParameterJdbcTemplate jdbc;
    private final StringRedisTemplate redis;
    private final AlertRepository alerts;
    private final AlertServiceProperties.Slo slo;
    private final AtomicReference<Double> lastP95 = new AtomicReference<>(0.0);

    public SloMonitorJob(NamedParameterJdbcTemplate jdbc, StringRedisTemplate redis, AlertRepository alerts,
                         AlertServiceProperties properties, MeterRegistry metrics) {
        this.jdbc = jdbc;
        this.redis = redis;
        this.alerts = alerts;
        this.slo = properties.slo();
        metrics.gauge("alerts.slo.p95_ms", lastP95, AtomicReference::get);
    }

    @Scheduled(fixedDelay = 30_000, initialDelay = 20_000)
    @SchedulerLock(name = "slo-monitor", lockAtMostFor = "PT25S")
    public void measure() {
        Instant now = Instant.now();
        Instant since = now.minus(slo.window());
        MapSqlParameterSource p = new MapSqlParameterSource()
                .addValue("since", Timestamp.from(since))
                .addValue("threshold", slo.latencyThresholdMs());
        Map<String, Object> latency = jdbc.queryForMap("""
                SELECT count(*) AS n,
                       percentile_cont(0.50) WITHIN GROUP (ORDER BY latency_ms) AS p50,
                       percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms) AS p95,
                       percentile_cont(0.99) WITHIN GROUP (ORDER BY latency_ms) AS p99,
                       max(latency_ms) AS max,
                       avg(CASE WHEN latency_ms <= :threshold THEN 1.0 ELSE 0.0 END) AS ratio
                FROM alert_revisions
                WHERE received_at >= :since AND status IN ('NEW', 'ESCALATED') AND severity IN ('HIGH', 'CRITICAL')
                """, p);
        Double freshness = jdbc.queryForObject("""
                SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (stored_at - event_time)) * 1000)
                FROM sensor_events WHERE event_time >= :recent
                """, new MapSqlParameterSource("recent", Timestamp.from(now.minus(Duration.ofMinutes(1)))), Double.class);

        long n = ((Number) latency.get("n")).longValue();
        Double p95 = number(latency.get("p95"));
        Double ratio = number(latency.get("ratio"));
        boolean latencyBreached = n > 0 && ratio != null && ratio < slo.targetRatio();
        boolean freshnessBreached = freshness != null && freshness > slo.freshnessThresholdMs();
        boolean breached = latencyBreached || freshnessBreached;
        lastP95.set(p95 == null ? 0.0 : p95);

        jdbc.update("""
                INSERT INTO slo_measurements (measured_at, window_seconds, alerts, p50_ms, p95_ms, p99_ms, max_ms,
                    within_slo_ratio, freshness_p95_ms, breached)
                VALUES (:at, :window, :n, :p50, :p95, :p99, :max, :ratio, :freshness, :breached)
                """, new MapSqlParameterSource()
                .addValue("at", Timestamp.from(now)).addValue("window", slo.window().toSeconds()).addValue("n", n)
                .addValue("p50", number(latency.get("p50"))).addValue("p95", p95)
                .addValue("p99", number(latency.get("p99"))).addValue("max", number(latency.get("max")))
                .addValue("ratio", ratio).addValue("freshness", freshness).addValue("breached", breached));

        if (breached) {
            onBreach(now, n, p95, ratio, freshness);
        } else if (Boolean.TRUE.equals(redis.delete(DEGRADED_KEY))) {
            log.info("SLO recuperado: se desactiva el modo degradado");
        }
    }

    private void onBreach(Instant now, long n, Double p95, Double ratio, Double freshness) {
        Boolean firstTime = redis.opsForValue().setIfAbsent(DEGRADED_KEY, "1", Duration.ofMinutes(2));
        if (Boolean.TRUE.equals(firstTime)) {
            String description = String.format(Locale.ROOT,
                    "SLO incumplido en los ultimos %d s: %d alertas, p95 de latencia %.0f ms (objetivo < %d ms en el "
                            + "%.0f %% de los casos, cumplimiento actual %.1f %%), frescura de datos p95 %.0f ms. "
                            + "Se activo el modo degradado: la ingesta muestrea lecturas de velocidad normales.",
                    slo.window().toSeconds(), n, p95 == null ? 0 : p95, slo.latencyThresholdMs(),
                    slo.targetRatio() * 100, ratio == null ? 100 : ratio * 100, freshness == null ? 0 : freshness);
            alerts.saveSystemAlert(UUID.randomUUID(), "SLO_INCUMPLIDO", "HIGH", "Degradacion del servicio", description, now);
            log.warn(description);
        } else {
            redis.expire(DEGRADED_KEY, Duration.ofMinutes(2));
        }
    }

    private static Double number(Object value) {
        return value == null ? null : ((Number) value).doubleValue();
    }
}
