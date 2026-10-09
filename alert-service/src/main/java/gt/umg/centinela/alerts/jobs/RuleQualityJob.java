package gt.umg.centinela.alerts.jobs;

import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Analisis de falsos positivos y ajuste adaptativo de umbrales (equivalente liviano a las tareas de
 * "analisis, validacion y entrenamiento" que el enunciado asigna a Airflow).
 *
 * <p>Con las validaciones de los operadores de los ultimos 7 dias calcula, por regla y zona:
 * <ul>
 *   <li>precision = confirmadas / validadas;</li>
 *   <li>omisiones = anclas clasificadas como "posible falso positivo" que el operador confirmo como reales.</li>
 * </ul>
 * Ajuste: si la precision es baja (&lt; 50 % con al menos 5 validaciones) el umbral sube +0.10 (se exige
 * mas evidencia); si hay 3 o mas omisiones y la precision es buena (&ge; 80 %) baja -0.05.
 * El resultado se publica en Redis y el motor de correlacion lo aplica en menos de 30 s.
 */
@Component
public class RuleQualityJob {

    private static final Logger log = LoggerFactory.getLogger(RuleQualityJob.class);
    static final String ADJUSTMENTS_KEY = "correlation:threshold-adjustments";
    private static final String MISSED_SUFFIX = "/sin-corroborar";

    private final JdbcTemplate jdbc;
    private final StringRedisTemplate redis;

    public RuleQualityJob(JdbcTemplate jdbc, StringRedisTemplate redis) {
        this.jdbc = jdbc;
        this.redis = redis;
    }

    record Stats(String ruleId, int zoneId, int validated, int confirmed, int falsePositives, int missed) {
        double precision() {
            return validated == 0 ? 1.0 : (double) confirmed / validated;
        }

        double adjustment() {
            if (validated >= 5 && precision() < 0.5) {
                return 0.10;
            }
            if (missed >= 3 && precision() >= 0.8) {
                return -0.05;
            }
            return 0.0;
        }
    }

    @Scheduled(fixedDelay = 3_600_000, initialDelay = 90_000)
    @SchedulerLock(name = "rule-quality", lockAtMostFor = "PT10M")
    public void compute() {
        List<Map<String, Object>> rows = jdbc.queryForList("""
                SELECT rule_id, zone_id,
                       count(*) AS validated,
                       count(*) FILTER (WHERE status = 'CONFIRMED') AS confirmed,
                       count(*) FILTER (WHERE status = 'FALSE_POSITIVE') AS false_positives
                FROM alerts
                WHERE validated_at > now() - INTERVAL '7 days' AND rule_id <> 'SISTEMA'
                GROUP BY rule_id, zone_id
                """);
        Map<String, Stats> stats = new HashMap<>();
        Map<String, Integer> missed = new HashMap<>();
        for (Map<String, Object> row : rows) {
            String rule = (String) row.get("rule_id");
            int zone = ((Number) row.get("zone_id")).intValue();
            int confirmed = ((Number) row.get("confirmed")).intValue();
            if (rule.endsWith(MISSED_SUFFIX)) {
                // un "posible falso positivo" confirmado como real = el motor omitio un incidente
                missed.merge(rule.substring(0, rule.length() - MISSED_SUFFIX.length()) + ":" + zone, confirmed, Integer::sum);
                continue;
            }
            stats.put(rule + ":" + zone, new Stats(rule, zone, ((Number) row.get("validated")).intValue(), confirmed,
                    ((Number) row.get("false_positives")).intValue(), 0));
        }
        missed.forEach((key, count) -> {
            String[] parts = key.split(":");
            stats.merge(key, new Stats(parts[0], Integer.parseInt(parts[1]), 0, 0, 0, count),
                    (a, b) -> new Stats(a.ruleId(), a.zoneId(), a.validated(), a.confirmed(), a.falsePositives(), b.missed()));
        });

        Map<String, String> adjustments = new HashMap<>();
        for (Stats s : stats.values()) {
            double adjustment = s.adjustment();
            jdbc.update("""
                    INSERT INTO rule_quality (rule_id, zone_id, computed_at, validated, confirmed, false_positives, missed,
                        precision, threshold_adjustment)
                    VALUES (?, ?, now(), ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (rule_id, zone_id) DO UPDATE SET computed_at = now(), validated = EXCLUDED.validated,
                        confirmed = EXCLUDED.confirmed, false_positives = EXCLUDED.false_positives,
                        missed = EXCLUDED.missed, precision = EXCLUDED.precision,
                        threshold_adjustment = EXCLUDED.threshold_adjustment
                    """, s.ruleId(), s.zoneId(), s.validated(), s.confirmed(), s.falsePositives(), s.missed(),
                    s.validated() == 0 ? null : s.precision(), adjustment);
            if (adjustment != 0.0) {
                adjustments.put(s.ruleId() + ":" + s.zoneId(), String.valueOf(adjustment));
            }
        }
        redis.delete(ADJUSTMENTS_KEY);
        if (!adjustments.isEmpty()) {
            redis.opsForHash().putAll(ADJUSTMENTS_KEY, adjustments);
        }
        log.info("Calidad de reglas calculada para {} combinaciones regla/zona; {} ajustes de umbral activos",
                stats.size(), adjustments.size());
    }
}
