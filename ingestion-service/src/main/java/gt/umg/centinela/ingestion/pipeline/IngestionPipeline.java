package gt.umg.centinela.ingestion.pipeline;

import gt.umg.centinela.common.EventHeaders;
import gt.umg.centinela.common.Topics;
import gt.umg.centinela.common.json.Json;
import gt.umg.centinela.common.model.NormalizedEvent;
import gt.umg.centinela.common.model.SensorType;
import gt.umg.centinela.ingestion.config.IngestionProperties;
import gt.umg.centinela.ingestion.config.ResilienceProperties;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.TimestampType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Flujo de ingesta de un evento: normalizar -> (degradar) -> deduplicar -> publicar.
 *
 * <p>La publicacion es sincronica: el offset del mensaje crudo solo se confirma cuando Kafka acepto
 * el evento normalizado (entrega "al menos una vez"). La marca de deduplicacion se pone "pendiente"
 * antes de publicar y pasa a "publicada" despues; si la publicacion falla se libera, y si el proceso
 * muere, la reentrega del mismo mensaje la reconoce como propia (ver {@link DedupStore}). Asi un
 * reintento nunca se descarta por error.
 */
@Component
public class IngestionPipeline {

    private final EventNormalizer normalizer;
    private final KafkaTemplate<String, String> kafka;
    private final DedupStore dedup;
    private final DegradationMonitor degradation;
    private final IngestionProperties properties;
    private final ResilienceProperties resilience;
    private final MeterRegistry metrics;
    private final Timer processingTimer;

    public IngestionPipeline(EventNormalizer normalizer, KafkaTemplate<String, String> kafka, DedupStore dedup,
                             DegradationMonitor degradation, IngestionProperties properties,
                             ResilienceProperties resilience, MeterRegistry metrics) {
        this.normalizer = normalizer;
        this.kafka = kafka;
        this.dedup = dedup;
        this.degradation = degradation;
        this.properties = properties;
        this.resilience = resilience;
        this.metrics = metrics;
        this.processingTimer = Timer.builder("ingestion.processing").description("Tiempo de procesamiento por evento")
                .publishPercentileHistogram().register(metrics);
    }

    /** Procesa un mensaje crudo de Kafka (con su hora de entrada al sistema y su identidad de entrega). */
    public void process(SensorType type, ConsumerRecord<String, String> record) {
        String token = record.topic() + ":" + record.partition() + ":" + record.offset();
        processingTimer.record(() -> doProcess(type, record.value(), reference(record), spooled(record), token));
    }

    public void process(SensorType type, String json) {
        processingTimer.record(() -> doProcess(type, json, null, false, UUID.randomUUID().toString()));
    }

    private void doProcess(SensorType type, String json, Instant reference, boolean spooled, String token) {
        NormalizedEvent event;
        try {
            event = normalizer.normalize(type, json, reference, spooled);
        } catch (RuntimeException e) {
            count(type, "invalid");
            throw e;
        }
        // Antes de deduplicar: en modo degradado ahorra un viaje a Redis por cada lectura descartada
        if (shouldShed(event)) {
            count(type, "shed");
            return;
        }
        DedupStore.Claim claim = dedup.claim(event.eventId(), token);
        if (claim == DedupStore.Claim.DUPLICADO) {
            count(type, "duplicate");
            return;
        }
        if (claim == DedupStore.Claim.REINTENTO_PROPIO) {
            metrics.counter("ingestion.dedup.reclaimed").increment();
        }
        try {
            kafka.send(Topics.NORMALIZED, String.valueOf(event.zoneId()), Json.write(event))
                    .get(resilience.publishTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            dedup.release(event.eventId(), token);
            throw new IllegalStateException("Interrumpido publicando " + event.eventId(), e);
        } catch (ExecutionException | TimeoutException | RuntimeException e) {
            // RuntimeException: send() lanza directo si no consigue metadatos del broker en max.block.ms
            dedup.release(event.eventId(), token);
            throw new IllegalStateException("No se pudo publicar el evento normalizado " + event.eventId(), e);
        }
        dedup.confirm(event.eventId(), token);
        count(type, "ok");
    }

    /** Hora de entrada al sistema: header del borde, o el timestamp de creacion del mensaje. */
    static Instant reference(ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader(EventHeaders.FIRST_ATTEMPT);
        if (header != null) {
            try {
                return Instant.ofEpochMilli(Long.parseLong(new String(header.value(), StandardCharsets.UTF_8).trim()));
            } catch (NumberFormatException ignored) {
                // header corrupto: se usa el timestamp del mensaje
            }
        }
        if (record.timestampType() == TimestampType.CREATE_TIME && record.timestamp() >= 0) {
            return Instant.ofEpochMilli(record.timestamp());
        }
        return null;
    }

    private static boolean spooled(ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader(EventHeaders.SPOOLED);
        return header != null && "true".equals(new String(header.value(), StandardCharsets.UTF_8));
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
