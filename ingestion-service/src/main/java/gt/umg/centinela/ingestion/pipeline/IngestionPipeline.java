package gt.umg.centinela.ingestion.pipeline;

import gt.umg.centinela.common.Topics;
import gt.umg.centinela.common.json.Json;
import gt.umg.centinela.common.model.NormalizedEvent;
import gt.umg.centinela.common.model.SensorType;
import gt.umg.centinela.ingestion.config.IngestionProperties;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Flujo de ingesta de un evento: normalizar -> deduplicar -> (degradar) -> publicar.
 *
 * <p>La publicacion es sincronica: el offset del mensaje crudo solo se confirma cuando Kafka acepto
 * el evento normalizado (entrega "al menos una vez"). Si algo falla se libera la marca de
 * deduplicacion para que el reintento no lo descarte por error.
 */
@Component
public class IngestionPipeline {

    private static final Logger log = LoggerFactory.getLogger(IngestionPipeline.class);

    private final EventNormalizer normalizer;
    private final KafkaTemplate<String, String> kafka;
    private final StringRedisTemplate redis;
    private final DegradationMonitor degradation;
    private final IngestionProperties properties;
    private final MeterRegistry metrics;
    private final Timer processingTimer;

    public IngestionPipeline(EventNormalizer normalizer, KafkaTemplate<String, String> kafka, StringRedisTemplate redis,
                             DegradationMonitor degradation, IngestionProperties properties, MeterRegistry metrics) {
        this.normalizer = normalizer;
        this.kafka = kafka;
        this.redis = redis;
        this.degradation = degradation;
        this.properties = properties;
        this.metrics = metrics;
        this.processingTimer = Timer.builder("ingestion.processing").description("Tiempo de procesamiento por evento")
                .publishPercentileHistogram().register(metrics);
    }

    public void process(SensorType type, String json) {
        processingTimer.record(() -> doProcess(type, json));
    }

    private void doProcess(SensorType type, String json) {
        NormalizedEvent event;
        try {
            event = normalizer.normalize(type, json);
        } catch (RuntimeException e) {
            count(type, "invalid");
            throw e;
        }
        if (!firstTimeSeen(event)) {
            count(type, "duplicate");
            return;
        }
        if (shouldShed(event)) {
            count(type, "shed");
            return;
        }
        try {
            kafka.send(Topics.NORMALIZED, String.valueOf(event.zoneId()), Json.write(event)).get(10, TimeUnit.SECONDS);
            count(type, "ok");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            releaseDedup(event);
            throw new IllegalStateException("Interrumpido publicando " + event.eventId(), e);
        } catch (ExecutionException | TimeoutException e) {
            releaseDedup(event);
            throw new IllegalStateException("No se pudo publicar el evento normalizado " + event.eventId(), e);
        }
    }

    /** Deduplicacion por event_id con SET NX + expiracion. Si Redis falla se prefiere disponibilidad. */
    private boolean firstTimeSeen(NormalizedEvent event) {
        try {
            Boolean fresh = redis.opsForValue().setIfAbsent("dedup:" + event.eventId(), "1", properties.dedupTtl());
            return !Boolean.FALSE.equals(fresh);
        } catch (RuntimeException e) {
            log.warn("Redis no disponible para deduplicar, se continua sin deduplicacion: {}", e.getMessage());
            return true;
        }
    }

    private void releaseDedup(NormalizedEvent event) {
        try {
            redis.delete("dedup:" + event.eventId());
        } catch (RuntimeException ignored) {
            // si Redis tampoco responde, el TTL limpiara la marca
        }
    }

    /**
     * Degradacion controlada (ADR-008): si el SLO de latencia se esta incumpliendo, se conserva solo
     * una muestra de las lecturas de velocidad normales. No afectan la correlacion (solo importan los
     * excesos) y son el mayor volumen del sistema.
     */
    private boolean shouldShed(NormalizedEvent event) {
        return degradation.isDegraded()
                && event.sensorType() == SensorType.SPEED
                && !event.anomalous()
                && ThreadLocalRandom.current().nextDouble() > properties.degradedSampleRate();
    }

    private void count(SensorType type, String result) {
        metrics.counter("ingestion.events", "sensor_type", type.name(), "result", result).increment();
    }
}
