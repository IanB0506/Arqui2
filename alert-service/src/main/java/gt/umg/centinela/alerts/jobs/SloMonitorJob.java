package gt.umg.centinela.alerts.jobs;

import gt.umg.centinela.alerts.config.AlertServiceProperties;
import gt.umg.centinela.alerts.persistence.AlertRepository;
import gt.umg.centinela.common.RedisKeys;
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
import java.util.List;
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
 * Si se incumple, no solo se pinta en rojo en Grafana: el sistema actua, segun la causa.
 * <ul>
 *   <li><b>SOBRECARGA</b> (el ultimo minuto tambien incumple y el atraso de los consumidores no baja):
 *       activa la bandera {@code system:degraded} en Redis (la ingesta muestrea lecturas de velocidad
 *       normales para liberar capacidad) y genera una alerta de sistema SLO_INCUMPLIDO.</li>
 *   <li><b>RECUPERACION</b> (el atraso va bajando, o el ultimo minuto ya cumple): son alertas que
 *       llegaron tarde por una caida ya superada. No se degrada (descartaria datos justo durante la
 *       puesta al dia); se avisa una vez al operador y se registra con su causa.</li>
 * </ul>
 * Toda medicion queda registrada (con causa y atraso) para el informe de cumplimiento.
 */
@Component
public class SloMonitorJob {

    private static final Logger log = LoggerFactory.getLogger(SloMonitorJob.class);
    static final String DEGRADED_KEY = RedisKeys.DEGRADED;
    private static final String RECOVERY_NOTICE_KEY = "system:slo-recovery-notice";

    private final NamedParameterJdbcTemplate jdbc;
    private final StringRedisTemplate redis;
    private final AlertRepository alerts;
    private final AlertServiceProperties.Slo slo;
    private final ConsumerLagProbe lagProbe;
    private final AtomicReference<Double> lastP95 = new AtomicReference<>(0.0);

    public SloMonitorJob(NamedParameterJdbcTemplate jdbc, StringRedisTemplate redis, AlertRepository alerts,
                         ConsumerLagProbe lagProbe, AlertServiceProperties properties, MeterRegistry metrics) {
        this.jdbc = jdbc;
        this.redis = redis;
        this.alerts = alerts;
        this.lagProbe = lagProbe;
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

        Long lag = lagProbe.totalLag();
        String cause = breached ? cause(now, lag, freshness) : null;

        jdbc.update("""
                INSERT INTO slo_measurements (measured_at, window_seconds, alerts, p50_ms, p95_ms, p99_ms, max_ms,
                    within_slo_ratio, freshness_p95_ms, breached, cause, consumer_lag)
                VALUES (:at, :window, :n, :p50, :p95, :p99, :max, :ratio, :freshness, :breached, :cause, :lag)
                """, new MapSqlParameterSource()
                .addValue("at", Timestamp.from(now)).addValue("window", slo.window().toSeconds()).addValue("n", n)
                .addValue("p50", number(latency.get("p50"))).addValue("p95", p95)
                .addValue("p99", number(latency.get("p99"))).addValue("max", number(latency.get("max")))
                .addValue("ratio", ratio).addValue("freshness", freshness).addValue("breached", breached)
                .addValue("cause", cause).addValue("lag", lag));

        if (OVERLOAD.equals(cause)) {
            onBreach(now, n, p95, ratio, freshness);
        } else if (RECOVERY.equals(cause)) {
            onRecoveryBreach(now, n, p95, lag);
        } else if (!breached && Boolean.TRUE.equals(redis.delete(DEGRADED_KEY))) {
            log.info("SLO recuperado: se desactiva el modo degradado");
        }
    }

    static final String OVERLOAD = "SOBRECARGA";
    static final String RECOVERY = "RECUPERACION";

    /**
     * Sobrecarga = el problema sigue ocurriendo ahora (el ultimo minuto tambien incumple) y la cola no se
     * esta vaciando. Si el atraso baja, o lo reciente ya cumple, son eventos que se atrasaron por una
     * caida ya superada.
     */
    private String cause(Instant now, Long lag, Double freshness) {
        Map<String, Object> recent = jdbc.queryForMap("""
                SELECT count(*) AS n, avg(CASE WHEN latency_ms <= :threshold THEN 1.0 ELSE 0.0 END) AS ratio
                FROM alert_revisions
                WHERE received_at >= :since AND status IN ('NEW', 'ESCALATED') AND severity IN ('HIGH', 'CRITICAL')
                """, new MapSqlParameterSource("since", Timestamp.from(now.minus(Duration.ofMinutes(1))))
                .addValue("threshold", slo.latencyThresholdMs()));
        long recentN = ((Number) recent.get("n")).longValue();
        Double recentRatio = number(recent.get("ratio"));
        boolean recentLatencyOk = recentN == 0 || (recentRatio != null && recentRatio >= slo.targetRatio());
        boolean freshnessOk = freshness == null || freshness <= slo.freshnessThresholdMs();
        Long previousLag = previousLag();
        boolean lagFalling = lag != null && previousLag != null && lag < previousLag;
        if (lagFalling || (recentLatencyOk && freshnessOk)) {
            return RECOVERY;
        }
        return OVERLOAD;
    }

    private Long previousLag() {
        List<Long> rows = jdbc.query("""
                SELECT consumer_lag FROM slo_measurements WHERE consumer_lag IS NOT NULL
                ORDER BY measured_at DESC LIMIT 1
                """, new MapSqlParameterSource(), (rs, i) -> rs.getLong(1));
        return rows.isEmpty() ? null : rows.get(0);
    }

    private void onRecoveryBreach(Instant now, long n, Double p95, Long lag) {
        Boolean first = redis.opsForValue().setIfAbsent(RECOVERY_NOTICE_KEY, "1", Duration.ofMinutes(10));
        if (!Boolean.TRUE.equals(first)) {
            return;
        }
        String description = String.format(Locale.ROOT,
                "Puesta al dia tras una caida: %d alertas de los ultimos %d s llegaron tarde (p95 %.0f ms) porque sus "
                        + "eventos esperaron en Kafka o en un spool mientras un nodo estaba caido. El atraso de los "
                        + "consumidores %s. No es una sobrecarga: no se activa el modo degradado.",
                n, slo.window().toSeconds(), p95 == null ? 0 : p95,
                lag == null ? "no se pudo medir" : "es de " + lag + " mensajes y va bajando");
        alerts.saveSystemAlert(UUID.randomUUID(), "SLO_INCUMPLIDO", "MEDIUM", "Puesta al dia tras una caida", description, now);
        log.warn(description);
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
