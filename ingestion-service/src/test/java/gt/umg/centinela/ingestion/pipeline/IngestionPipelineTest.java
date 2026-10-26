package gt.umg.centinela.ingestion.pipeline;

import gt.umg.centinela.common.EventHeaders;
import gt.umg.centinela.common.model.SensorType;
import gt.umg.centinela.common.privacy.PlatePseudonymizer;
import gt.umg.centinela.ingestion.config.IngestionProperties;
import gt.umg.centinela.ingestion.config.ResilienceProperties;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.validation.Validation;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class IngestionPipelineTest {

    private static final Instant NOW = Instant.parse("2026-10-30T04:05:00Z");

    /** Replica en memoria la semantica de los scripts Lua de RedisDedupStore. */
    static final class InMemoryDedupStore implements DedupStore {
        final Map<UUID, String> marks = new HashMap<>();

        @Override
        public Claim claim(UUID id, String token) {
            String cur = marks.get(id);
            if (cur == null) {
                marks.put(id, token + "|P");
                return Claim.NUEVO;
            }
            return cur.equals(token + "|P") ? Claim.REINTENTO_PROPIO : Claim.DUPLICADO;
        }

        @Override
        public void confirm(UUID id, String token) {
            String cur = marks.get(id);
            if (cur == null || cur.equals(token + "|P")) {
                marks.put(id, token + "|OK");
            }
        }

        @Override
        public void release(UUID id, String token) {
            marks.remove(id, token + "|P");
        }
    }

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
    private final InMemoryDedupStore dedup = new InMemoryDedupStore();
    private final SimpleMeterRegistry metrics = new SimpleMeterRegistry();
    private final IngestionPipeline pipeline;

    IngestionPipelineTest() {
        IngestionProperties properties = new IngestionProperties("secreto-de-pruebas-123", 0.3, 60, 3,
                Duration.ofMinutes(2), Duration.ofHours(24), false, Duration.ofMinutes(10), 0.1);
        ResilienceProperties resilience = new ResilienceProperties(Duration.ofSeconds(1), Duration.ofSeconds(30), 4, 10,
                Duration.ofSeconds(2), Duration.ofSeconds(5), Duration.ofSeconds(60), Duration.ofMinutes(2));
        EventNormalizer normalizer = new EventNormalizer(properties, new PlatePseudonymizer(properties.plateHmacSecret()),
                Validation.buildDefaultValidatorFactory().getValidator(), Clock.fixed(NOW, ZoneOffset.UTC));
        DegradationMonitor degradation = mock(DegradationMonitor.class);
        pipeline = new IngestionPipeline(normalizer, kafka, dedup, degradation, properties, resilience, metrics);
    }

    private static ConsumerRecord<String, String> record(long offset) {
        String json = """
                {"event_id":"11111111-2222-3333-4444-555555555555","timestamp":"%s","sensor_id":"VEL-04-1",
                 "zona":"Zona 4","velocidad_detectada":85,"direccion":"NORTE","limite_velocidad":60}
                """.formatted(NOW.minusSeconds(5));
        ConsumerRecord<String, String> r = new ConsumerRecord<>("sensor.speed.raw", 3, offset, "VEL-04-1", json);
        r.headers().add(EventHeaders.FIRST_ATTEMPT, Long.toString(NOW.minusSeconds(5).toEpochMilli()).getBytes(StandardCharsets.UTF_8));
        return r;
    }

    private double count(String result) {
        return metrics.counter("ingestion.events", "sensor_type", "SPEED", "result", result).count();
    }

    @Test
    void siLaPublicacionFallaLaMarcaSeLiberaYElReintentoPublica() {
        when(kafka.send(anyString(), anyString(), anyString()))
                .thenThrow(new KafkaException("sin metadatos del broker"))
                .thenReturn(CompletableFuture.completedFuture(null));
        assertThatThrownBy(() -> pipeline.process(SensorType.SPEED, record(7))).isInstanceOf(IllegalStateException.class);
        pipeline.process(SensorType.SPEED, record(7));
        verify(kafka, times(2)).send(anyString(), anyString(), anyString());
        assertThat(count("ok")).isEqualTo(1);
    }

    @Test
    void laReentregaDelMismoMensajeTrasUnaMuerteAbruptaNoSeDescarta() {
        dedup.claim(UUID.fromString("11111111-2222-3333-4444-555555555555"), "sensor.speed.raw:3:7"); // quedo pendiente
        when(kafka.send(anyString(), anyString(), anyString())).thenReturn(CompletableFuture.completedFuture(null));
        pipeline.process(SensorType.SPEED, record(7));
        assertThat(count("ok")).isEqualTo(1);
        assertThat(metrics.counter("ingestion.dedup.reclaimed").count()).isEqualTo(1);
    }

    @Test
    void unaCopiaEnOtroOffsetOUnaReentregaYaPublicadaSonDuplicados() {
        when(kafka.send(anyString(), anyString(), anyString())).thenReturn(CompletableFuture.completedFuture(null));
        pipeline.process(SensorType.SPEED, record(7));
        pipeline.process(SensorType.SPEED, record(7));
        pipeline.process(SensorType.SPEED, record(9));
        verify(kafka, times(1)).send(anyString(), anyString(), anyString());
        assertThat(count("duplicate")).isEqualTo(2);
    }

    @Test
    void sinRedisSePublicaIgual() {
        DedupStore down = new DedupStore() {
            public Claim claim(UUID id, String token) { return Claim.SIN_DEDUPLICACION; }
            public void confirm(UUID id, String token) { }
            public void release(UUID id, String token) { }
        };
        when(kafka.send(anyString(), anyString(), anyString())).thenReturn(CompletableFuture.completedFuture(null));
        IngestionProperties properties = new IngestionProperties("secreto-de-pruebas-123", 0.3, 60, 3,
                Duration.ofMinutes(2), Duration.ofHours(24), false, Duration.ofMinutes(10), 0.1);
        IngestionPipeline withoutRedis = new IngestionPipeline(new EventNormalizer(properties,
                new PlatePseudonymizer(properties.plateHmacSecret()), Validation.buildDefaultValidatorFactory().getValidator(),
                Clock.fixed(NOW, ZoneOffset.UTC)), kafka, down, mock(DegradationMonitor.class), properties,
                new ResilienceProperties(Duration.ofSeconds(1), Duration.ofSeconds(30), 4, 10, Duration.ofSeconds(2),
                        Duration.ofSeconds(5), Duration.ofSeconds(60), Duration.ofMinutes(2)), metrics);
        withoutRedis.process(SensorType.SPEED, record(7));
        assertThat(count("ok")).isEqualTo(1);
    }
}
