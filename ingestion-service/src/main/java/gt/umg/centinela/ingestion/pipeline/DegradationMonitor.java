package gt.umg.centinela.ingestion.pipeline;

import gt.umg.centinela.ingestion.config.ResilienceProperties;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

/**
 * Lee cada 5 s la bandera {@code system:degraded} que el alert-service activa cuando el SLO de
 * latencia se incumple por sobrecarga. Se cachea en memoria para no consultar Redis en cada evento.
 *
 * <p>Si Redis deja de responder, la bandera no se queda congelada: pasado su TTL (2 min) sin poder
 * leerla, se deja de muestrear. La lectura tambien sirve de sonda para el disyuntor de Redis.
 */
@Component
public class DegradationMonitor {

    private static final Logger log = LoggerFactory.getLogger(DegradationMonitor.class);
    public static final String DEGRADED_KEY = "system:degraded";

    private final StringRedisTemplate redis;
    private final RedisAvailability availability;
    private final Duration staleAfter;
    private final Clock clock;
    private volatile boolean degraded;
    private volatile Instant lastRead;

    public DegradationMonitor(StringRedisTemplate redis, RedisAvailability availability, ResilienceProperties resilience,
                              Clock clock, MeterRegistry metrics) {
        this.redis = redis;
        this.availability = availability;
        this.staleAfter = resilience.degradedStaleAfter();
        this.clock = clock;
        this.lastRead = clock.instant();
        metrics.gauge("ingestion.degraded", this, m -> m.degraded ? 1 : 0);
    }

    @Scheduled(fixedDelay = 5000)
    public void refresh() {
        try {
            boolean now = "1".equals(redis.opsForValue().get(DEGRADED_KEY));
            availability.success();
            lastRead = clock.instant();
            if (now != degraded) {
                log.warn("Modo degradado {}", now ? "ACTIVADO: se muestrean lecturas de velocidad normales" : "desactivado");
            }
            degraded = now;
        } catch (RuntimeException e) {
            availability.failure(e);
            if (degraded && Duration.between(lastRead, clock.instant()).compareTo(staleAfter) > 0) {
                degraded = false;
                log.warn("No se puede leer la bandera de degradacion desde hace mas de {} s: se deja de muestrear",
                        staleAfter.toSeconds());
            }
        }
    }

    public boolean isDegraded() {
        return degraded;
    }
}
