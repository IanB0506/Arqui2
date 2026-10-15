package gt.umg.centinela.ingestion.pipeline;

import gt.umg.centinela.ingestion.config.IngestionProperties;
import gt.umg.centinela.ingestion.config.ResilienceProperties;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

/**
 * {@link DedupStore} en Redis con scripts Lua (atomicos). Llave {@code dedup:<event_id>}, valor
 * {@code <token>|P} (pendiente, TTL corto) o {@code <token>|OK} (publicado, TTL de deduplicacion).
 * Una marca huerfana (el proceso murio con la marca pendiente) expira en {@code dedup-pending-ttl}.
 */
@Component
public class RedisDedupStore implements DedupStore {

    private static final DefaultRedisScript<String> CLAIM = new DefaultRedisScript<>("""
            local cur = redis.call('GET', KEYS[1])
            if not cur then
              redis.call('SET', KEYS[1], ARGV[1] .. '|P', 'PX', ARGV[2])
              return 'NUEVO'
            end
            if cur == ARGV[1] .. '|P' then
              redis.call('PEXPIRE', KEYS[1], ARGV[2])
              return 'REINTENTO_PROPIO'
            end
            return 'DUPLICADO'
            """, String.class);

    private static final DefaultRedisScript<Long> CONFIRM = new DefaultRedisScript<>("""
            local cur = redis.call('GET', KEYS[1])
            if (not cur) or cur == ARGV[1] .. '|P' then
              redis.call('SET', KEYS[1], ARGV[1] .. '|OK', 'PX', ARGV[2])
              return 1
            end
            return 0
            """, Long.class);

    private static final DefaultRedisScript<Long> RELEASE = new DefaultRedisScript<>("""
            if redis.call('GET', KEYS[1]) == ARGV[1] .. '|P' then
              return redis.call('DEL', KEYS[1])
            end
            return 0
            """, Long.class);

    private final StringRedisTemplate redis;
    private final RedisAvailability availability;
    private final String dedupTtlMs;
    private final String pendingTtlMs;
    private final Counter skipped;

    public RedisDedupStore(StringRedisTemplate redis, RedisAvailability availability, IngestionProperties properties,
                           ResilienceProperties resilience, MeterRegistry metrics) {
        this.redis = redis;
        this.availability = availability;
        this.dedupTtlMs = String.valueOf(properties.dedupTtl().toMillis());
        this.pendingTtlMs = String.valueOf(resilience.dedupPendingTtl().toMillis());
        this.skipped = metrics.counter("ingestion.dedup.skipped");
    }

    @Override
    public Claim claim(UUID eventId, String deliveryToken) {
        if (!availability.shouldTry()) {
            skipped.increment();
            return Claim.SIN_DEDUPLICACION;
        }
        try {
            String result = redis.execute(CLAIM, List.of(key(eventId)), deliveryToken, pendingTtlMs);
            availability.success();
            return result == null ? Claim.SIN_DEDUPLICACION : Claim.valueOf(result);
        } catch (RuntimeException e) {
            availability.failure(e);
            skipped.increment();
            return Claim.SIN_DEDUPLICACION;
        }
    }

    @Override
    public void confirm(UUID eventId, String deliveryToken) {
        run(CONFIRM, eventId, deliveryToken, dedupTtlMs);
    }

    @Override
    public void release(UUID eventId, String deliveryToken) {
        run(RELEASE, eventId, deliveryToken, "0");
    }

    private void run(DefaultRedisScript<Long> script, UUID eventId, String token, String ttl) {
        if (!availability.shouldTry()) {
            return; // a mejor esfuerzo: si Redis no esta, la marca pendiente expira sola
        }
        try {
            redis.execute(script, List.of(key(eventId)), token, ttl);
            availability.success();
        } catch (RuntimeException e) {
            availability.failure(e);
        }
    }

    private static String key(UUID eventId) {
        return "dedup:" + eventId;
    }
}
