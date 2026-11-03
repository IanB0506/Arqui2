package gt.umg.centinela.ingestion.pipeline;

import gt.umg.centinela.ingestion.config.ResilienceProperties;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.concurrent.atomic.AtomicLong;

@Component
// Con Redis caído, un solo hilo vuelve a probar cada pocos segundos en vez de que todos esperen su timeout.
public class RedisAvailability {
    private static final Logger log = LoggerFactory.getLogger(RedisAvailability.class);

    private final Clock clock;
    private final long retryAfterMs;
    private final AtomicLong nextTrial = new AtomicLong();
    private volatile boolean available = true;

    public RedisAvailability(ResilienceProperties properties, Clock clock, MeterRegistry metrics) {
        this.clock = clock;
        this.retryAfterMs = properties.redisRetryAfter().toMillis();
        metrics.gauge("ingestion.redis.available", this, r -> r.available ? 1 : 0);
    }

    public boolean shouldTry() {
        if (available) {
            return true;
        }
        long now = clock.millis();
        long next = nextTrial.get();
        return now >= next && nextTrial.compareAndSet(next, now + retryAfterMs);
    }

    public void success() {
        if (!available) {
            available = true;
            log.info("Redis disponible de nuevo: se reanuda la deduplicacion");
        }
    }

    public void failure(RuntimeException e) {
        nextTrial.set(clock.millis() + retryAfterMs);
        if (available) {
            available = false;
            log.warn("Redis no disponible ({}): la ingesta sigue sin deduplicar y reintenta cada {} s",
                    e.getMessage(), retryAfterMs / 1000);
        }
    }

    public boolean isAvailable() {
        return available;
    }
}
