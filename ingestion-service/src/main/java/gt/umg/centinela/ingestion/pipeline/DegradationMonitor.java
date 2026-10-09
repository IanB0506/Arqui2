package gt.umg.centinela.ingestion.pipeline;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Lee cada 5 s la bandera {@code system:degraded} que el alert-service activa cuando el SLO de
 * latencia se incumple. Se cachea en memoria para no consultar Redis en cada evento.
 */
@Component
public class DegradationMonitor {

    private static final Logger log = LoggerFactory.getLogger(DegradationMonitor.class);
    public static final String DEGRADED_KEY = "system:degraded";

    private final StringRedisTemplate redis;
    private volatile boolean degraded;

    public DegradationMonitor(StringRedisTemplate redis, MeterRegistry metrics) {
        this.redis = redis;
        metrics.gauge("ingestion.degraded", this, m -> m.degraded ? 1 : 0);
    }

    @Scheduled(fixedDelay = 5000)
    public void refresh() {
        try {
            boolean now = "1".equals(redis.opsForValue().get(DEGRADED_KEY));
            if (now != degraded) {
                log.warn("Modo degradado {}", now ? "ACTIVADO: se muestrean lecturas de velocidad normales" : "desactivado");
            }
            degraded = now;
        } catch (RuntimeException e) {
            log.debug("No se pudo leer bandera de degradacion: {}", e.getMessage());
        }
    }

    public boolean isDegraded() {
        return degraded;
    }
}
