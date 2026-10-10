package gt.umg.centinela.correlation.engine;

import gt.umg.centinela.common.json.Json;
import gt.umg.centinela.common.model.NormalizedEvent;
import gt.umg.centinela.common.model.SensorType;
import gt.umg.centinela.correlation.config.CorrelationProperties;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;

/**
 * Ventanas en Redis usando sorted sets: llave {@code win:{zona}:{tipo}}, puntaje = hora del evento
 * en milisegundos, miembro = evento en JSON. Consultar "eventos de la Zona 4 de tipo SPEED entre
 * 22:03 y 22:10" es un ZRANGEBYSCORE de complejidad O(log n), en memoria.
 */
@Component
public class RedisWindowStore implements WindowStore {

    private final StringRedisTemplate redis;
    private final Duration retention;

    public RedisWindowStore(StringRedisTemplate redis, CorrelationProperties properties) {
        this.redis = redis;
        this.retention = properties.getWindowRetention();
    }

    /** Agrega el evento, recorta lo viejo y renueva la expiracion en UN solo viaje de red (pipeline). */
    @Override
    public void add(NormalizedEvent event) {
        byte[] key = key(event.zoneId(), event.sensorType()).getBytes(StandardCharsets.UTF_8);
        byte[] member = Json.write(event).getBytes(StandardCharsets.UTF_8);
        long score = event.eventTime().toEpochMilli();
        redis.executePipelined((RedisCallback<Object>) connection -> {
            connection.zSetCommands().zAdd(key, score, member);
            connection.zSetCommands().zRemRangeByScore(key, Double.NEGATIVE_INFINITY, score - retention.toMillis());
            connection.keyCommands().pExpire(key, retention.multipliedBy(2).toMillis());
            return null;
        });
    }

    @Override
    public List<NormalizedEvent> range(int zoneId, SensorType type, Instant from, Instant to) {
        Set<String> members = redis.opsForZSet().rangeByScore(key(zoneId, type), from.toEpochMilli(), to.toEpochMilli());
        if (members == null) {
            return List.of();
        }
        return members.stream().map(json -> Json.read(json, NormalizedEvent.class)).toList();
    }

    private static String key(int zoneId, SensorType type) {
        return "win:" + zoneId + ":" + type.name();
    }
}
