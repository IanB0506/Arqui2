package gt.umg.centinela.ingestion.api;

import gt.umg.centinela.common.EventHeaders;
import gt.umg.centinela.common.kafka.FailureClassifier;
import gt.umg.centinela.common.spool.DiskSpool;
import gt.umg.centinela.common.spool.SpoolEntry;
import gt.umg.centinela.common.spool.SpoolOptions;
import gt.umg.centinela.common.spool.SpoolStats;
import gt.umg.centinela.ingestion.config.GatewayProperties;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Spool del gateway REST (ADR-006): si Kafka no confirma un panico o un reporte de la app en unos
 * segundos, el evento se guarda en disco (con fsync) y se responde 202 EN_COLA; se reenvia solo cuando
 * Kafka vuelve. Solo se responde 202 cuando el evento ya es durable (confirmado por Kafka o en disco);
 * si ni eso es posible se responde 503 y la app reintenta con el mismo event_id.
 */
@Component
public class GatewaySpool {

    private static final Logger log = LoggerFactory.getLogger(GatewaySpool.class);

    public enum Outcome { PUBLICADO, EN_COLA, RECHAZADO }

    private final KafkaTemplate<String, String> kafka;
    private final GatewayProperties config;
    private final Clock clock;
    private final DiskSpool spool;
    private final MeterRegistry metrics;
    private volatile long kafkaDownUntil;

    public GatewaySpool(KafkaTemplate<String, String> kafka, GatewayProperties config, Clock clock, MeterRegistry metrics) {
        this.kafka = kafka;
        this.config = config;
        this.clock = clock;
        this.metrics = metrics;
        try {
            this.spool = new DiskSpool(new SpoolOptions("gateway", Path.of(config.spoolDir(), "gateway.wal"), config.fsync(),
                    config.maxSizeMb() * 1024L * 1024L, null, SpoolOptions.Overflow.REJECT_NEW, config.maxAttempts()),
                    FailureClassifier::isPermanent, clock);
        } catch (IOException e) {
            throw new UncheckedIOException("No se pudo abrir el spool del gateway en " + config.spoolDir(), e);
        }
        Gauge.builder("ingestion.gateway.spool.pending", spool, DiskSpool::pending).register(metrics);
    }

    public Outcome submit(String topic, String eventId, String key, String json) {
        SpoolEntry entry = new SpoolEntry(eventId, topic, key, clock.millis(), 0, json);
        Outcome outcome;
        if (clock.millis() < kafkaDownUntil) {
            // Kafka se dio por caido hace poco: no hacer esperar a cada peticion
            outcome = spool.append(entry, false) ? Outcome.EN_COLA : rejected(eventId);
        } else {
            outcome = sendOrSpool(entry);
        }
        metrics.counter("ingestion.gateway.requests", "result", outcome.name()).increment();
        return outcome;
    }

    private Outcome sendOrSpool(SpoolEntry entry) {
        CompletableFuture<?> future;
        try {
            future = kafka.send(record(entry, false));
        } catch (RuntimeException e) {
            // send() lanza directo si no consigue metadatos del broker en max.block.ms
            markDown(e);
            return spool.append(entry, false) ? Outcome.EN_COLA : rejected(entry.id());
        }
        try {
            future.get(config.ackTimeout().toMillis(), TimeUnit.MILLISECONDS);
            return Outcome.PUBLICADO;
        } catch (TimeoutException e) {
            // El envio sigue en vuelo: se guarda y, si la confirmacion llega tarde, se borra sin reenviarlo
            markDown(e);
            if (!spool.append(entry, true)) {
                return rejected(entry.id());
            }
            future.whenComplete((result, error) -> {
                if (error == null) {
                    spool.ack(entry.id());
                } else {
                    spool.nack(entry.id(), error);
                }
            });
            return Outcome.EN_COLA;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return spool.append(entry, false) ? Outcome.EN_COLA : rejected(entry.id());
        } catch (ExecutionException e) {
            markDown(e);
            return spool.append(entry, false) ? Outcome.EN_COLA : rejected(entry.id());
        }
    }

    private Outcome rejected(String eventId) {
        log.error("EVENTO CRITICO NO ACEPTADO {}: Kafka y el spool en disco fallaron", eventId);
        return Outcome.RECHAZADO;
    }

    private void markDown(Throwable cause) {
        boolean wasUp = clock.millis() >= kafkaDownUntil;
        kafkaDownUntil = clock.millis() + config.kafkaDownBackoff().toMillis();
        if (wasUp) {
            log.warn("Kafka no disponible para el gateway ({}): los eventos criticos se guardan en el spool",
                    FailureClassifier.rootCause(cause).toString());
        }
    }

    @Scheduled(fixedDelayString = "${ingestion.gateway.resend-interval-ms:10000}", initialDelay = 10_000)
    public void resend() {
        if (spool.pending() == 0) {
            return;
        }
        DiskSpool.ResendResult r = spool.resend(e -> kafka.send(record(e, true)), 500, Duration.ofSeconds(5), 20);
        spool.compactIfNeeded();
        if (r.sent() > 0) {
            kafkaDownUntil = 0;
            log.info("Gateway: {} eventos criticos reenviados desde el spool, {} pendientes", r.sent(), r.remaining());
        }
    }

    private static ProducerRecord<String, String> record(SpoolEntry entry, boolean fromSpool) {
        ProducerRecord<String, String> record = new ProducerRecord<>(entry.topic(), null, entry.createdAtMs(),
                entry.key(), entry.payload());
        record.headers().add(EventHeaders.FIRST_ATTEMPT, Long.toString(entry.createdAtMs()).getBytes(StandardCharsets.UTF_8));
        if (fromSpool) {
            record.headers().add(EventHeaders.SPOOLED, "true".getBytes(StandardCharsets.UTF_8));
        }
        return record;
    }

    public Map<String, Object> status() {
        SpoolStats s = spool.stats();
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("kafkaDisponible", clock.millis() >= kafkaDownUntil);
        status.put("spoolPendientes", s.pending() + s.inFlight());
        status.put("enVuelo", s.inFlight());
        status.put("reenviados", s.resent());
        status.put("apartados", s.rejected());
        status.put("rechazadosPorTope", s.refused());
        status.put("lineasInvalidas", s.invalidLines());
        status.put("ultimoError", s.lastError());
        return status;
    }

    @PreDestroy
    public void close() {
        spool.close();
    }
}
