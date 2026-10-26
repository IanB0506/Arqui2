package gt.umg.centinela.correlation.engine;

import gt.umg.centinela.common.RedisKeys;
import gt.umg.centinela.common.json.Json;
import gt.umg.centinela.common.model.NormalizedEvent;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Implementacion en Redis. Llaves usadas:
 * <pre>
 *   incident:{tipo}:{zona}            estado del incidente abierto (TTL = cooldown)
 *   corroborated:{eventId}            el ancla ya formo parte de una alerta
 *   pending:{regla}                   sorted set de anclas esperando corroboracion (puntaje = plazo)
 *   devpanic:{sensor} / devflag:{s}   contador y bandera de abuso de boton de panico
 *   suspect:{placaHash}               vehiculo sospechoso en seguimiento
 *   watchlist:plates                  set de seudonimos de placas robadas (lo carga alert-service)
 *   watchlist:synced                  centinela: la lista esta cargada (si falta, Redis volvio vacio)
 *   correlation:backlog               alguna instancia procesa atraso: se posponen los falsos positivos
 *   correlation:threshold-adjustments hash con ajustes de umbral por regla y zona
 * </pre>
 */
@Component
public class RedisCorrelationState implements CorrelationState {

    public static final String WATCHLIST_KEY = RedisKeys.WATCHLIST;
    public static final String ADJUSTMENTS_KEY = RedisKeys.THRESHOLD_ADJUSTMENTS;

    private static final Logger log = LoggerFactory.getLogger(RedisCorrelationState.class);

    private final StringRedisTemplate redis;
    private volatile Set<String> watchlist = Set.of();
    private volatile boolean watchlistSynced;

    public RedisCorrelationState(StringRedisTemplate redis, MeterRegistry metrics) {
        this.redis = redis;
        metrics.gauge("correlation.watchlist.size", this, s -> s.watchlist.size());
        metrics.gauge("correlation.watchlist.synced", this, s -> s.watchlistSynced ? 1 : 0);
    }

    @Override
    public Optional<IncidentState> incident(String key) {
        String json = redis.opsForValue().get("incident:" + key);
        return json == null ? Optional.empty() : Optional.of(Json.read(json, IncidentState.class));
    }

    @Override
    public void saveIncident(String key, IncidentState state, Duration ttl) {
        redis.opsForValue().set("incident:" + key, Json.write(state), ttl);
    }

    @Override
    public void markCorroborated(UUID eventId, Duration ttl) {
        redis.opsForValue().set("corroborated:" + eventId, "1", ttl);
    }

    @Override
    public boolean isCorroborated(UUID eventId) {
        return Boolean.TRUE.equals(redis.hasKey("corroborated:" + eventId));
    }

    @Override
    public void addPending(String ruleId, NormalizedEvent anchor, Instant deadline) {
        redis.opsForZSet().add("pending:" + ruleId, Json.write(anchor), deadline.toEpochMilli());
    }

    @Override
    public List<NormalizedEvent> claimExpiredPending(String ruleId, Instant now, int limit) {
        String key = "pending:" + ruleId;
        Set<String> expired = redis.opsForZSet().rangeByScore(key, 0, now.toEpochMilli(), 0, limit);
        List<NormalizedEvent> claimed = new ArrayList<>();
        if (expired == null) {
            return claimed;
        }
        for (String json : expired) {
            Long removed = redis.opsForZSet().remove(key, json);
            if (removed != null && removed == 1) { // solo la instancia que logra borrarlo lo procesa
                claimed.add(Json.read(json, NormalizedEvent.class));
            }
        }
        return claimed;
    }

    @Override
    public long incrementDevicePanics(String sensorId, Duration window) {
        String key = "devpanic:" + sensorId;
        Long count = redis.opsForValue().increment(key);
        if (count != null && count == 1) {
            redis.expire(key, window);
        }
        return count == null ? 0 : count;
    }

    @Override
    public boolean flagDevice(String sensorId, Duration ttl) {
        return Boolean.TRUE.equals(redis.opsForValue().setIfAbsent("devflag:" + sensorId, "1", ttl));
    }

    @Override
    public boolean isDeviceFlagged(String sensorId) {
        return Boolean.TRUE.equals(redis.hasKey("devflag:" + sensorId));
    }

    @Override
    public void registerSuspect(String plateHash, SuspectState suspect, Duration ttl) {
        redis.opsForValue().set("suspect:" + plateHash, Json.write(suspect), ttl);
    }

    @Override
    public Optional<SuspectState> suspect(String plateHash) {
        String json = redis.opsForValue().get("suspect:" + plateHash);
        return json == null ? Optional.empty() : Optional.of(Json.read(json, SuspectState.class));
    }

    @Override
    public boolean firstSuspectSightingInZone(UUID alertId, int zoneId, Duration ttl) {
        return Boolean.TRUE.equals(redis.opsForValue().setIfAbsent("suspect-zone:" + alertId + ":" + zoneId, "1", ttl));
    }

    @Override
    public boolean suspectSeenInZone(UUID alertId, int zoneId) {
        return Boolean.TRUE.equals(redis.hasKey("suspect-zone:" + alertId + ":" + zoneId));
    }

    @Override
    public void markBacklog(Duration ttl) {
        redis.opsForValue().set(RedisKeys.CORRELATION_BACKLOG, "1", ttl);
    }

    @Override
    public boolean isBacklogged() {
        return Boolean.TRUE.equals(redis.hasKey(RedisKeys.CORRELATION_BACKLOG));
    }

    /**
     * Se consulta en cada lectura LPR (cientos por segundo), por eso se usa una copia local que se
     * refresca cada 30 s en lugar de preguntar a Redis cada vez. La lista cambia muy poco.
     */
    @Override
    public boolean isWatchlisted(String plateHash) {
        return watchlist.contains(plateHash);
    }

    @Scheduled(fixedDelay = 15_000)
    public void refreshWatchlist() {
        try {
            boolean synced = Boolean.TRUE.equals(redis.hasKey(RedisKeys.WATCHLIST_SYNCED));
            Set<String> members = redis.opsForSet().members(WATCHLIST_KEY);
            Set<String> latest = members == null ? Set.of() : Set.copyOf(members);
            if (!synced && latest.isEmpty() && !watchlist.isEmpty()) {
                // Redis volvio vacio (no persiste): se conserva la copia buena hasta que alert-service recargue
                if (watchlistSynced) {
                    log.warn("La lista de placas robadas no esta en Redis (Redis reiniciado?): se conserva la copia "
                            + "local de {} placas hasta que alert-service la recargue", watchlist.size());
                }
                watchlistSynced = false;
                return;
            }
            if (synced && !watchlistSynced) {
                log.info("Lista de placas robadas sincronizada: {} placas", latest.size());
            }
            watchlist = latest;
            watchlistSynced = synced;
        } catch (RuntimeException e) {
            log.warn("No se pudo refrescar la lista de placas robadas: {}", e.getMessage());
        }
    }

    @Override
    public Map<String, Double> thresholdAdjustments() {
        Map<String, Double> result = new HashMap<>();
        redis.<String, String>opsForHash().entries(ADJUSTMENTS_KEY)
                .forEach((k, v) -> result.put(k, Double.parseDouble(v)));
        return result;
    }
}
