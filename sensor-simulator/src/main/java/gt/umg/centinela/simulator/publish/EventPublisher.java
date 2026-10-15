package gt.umg.centinela.simulator.publish;

import gt.umg.centinela.common.json.Json;
import gt.umg.centinela.common.model.SensorType;
import gt.umg.centinela.common.spool.SpoolEntry;
import gt.umg.centinela.simulator.generator.SimulatedEvent;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Publica eventos simulados en Kafka.
 *
 * <p>Politica de perdida aceptable (ADR-006, ver {@link SpoolPolicy}):
 * <ul>
 *   <li>Los eventos criticos se escriben en disco ANTES de enviarlos (write-ahead). Si el proceso
 *       muere o Kafka no confirma, se reenvian despues. Perdida cero.</li>
 *   <li>La telemetria util se guarda solo si el envio falla, acotada por tamano y antiguedad.</li>
 *   <li>Las lecturas normales se descartan si Kafka no esta: no corroboran nada.</li>
 * </ul>
 * Con Kafka caido no se escribe un log por evento: un aviso al empezar y un resumen cada 30 s.
 */
@Component
public class EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(EventPublisher.class);

    private final KafkaTemplate<String, String> kafka;
    private final SpoolService spool;
    private final SpoolPolicy policy;
    private final MeterRegistry metrics;
    private final Clock clock;

    private final AtomicBoolean failing = new AtomicBoolean(false);
    private final AtomicLong criticalSpooled = new AtomicLong();
    private final AtomicLong telemetrySpooled = new AtomicLong();
    private final AtomicLong dropped = new AtomicLong();
    private volatile Instant lastAckAt;
    private volatile Instant lastFailureAt;
    private volatile String lastError;

    @Autowired
    public EventPublisher(KafkaTemplate<String, String> kafka, SpoolService spool, SpoolPolicy policy,
                          MeterRegistry metrics) {
        this(kafka, spool, policy, metrics, Clock.systemUTC());
    }

    EventPublisher(KafkaTemplate<String, String> kafka, SpoolService spool, SpoolPolicy policy, MeterRegistry metrics,
                   Clock clock) {
        this.kafka = kafka;
        this.spool = spool;
        this.policy = policy;
        this.metrics = metrics;
        this.clock = clock;
    }

    public void publish(SimulatedEvent event) {
        SpoolPolicy.Decision decision = policy.classify(event);
        SpoolEntry entry = new SpoolEntry(event.eventId().toString(), event.type().rawTopic(), event.key(),
                clock.millis(), 0, Json.write(event.payload()));
        boolean journaled = decision == SpoolPolicy.Decision.CRITICAL && spool.writeAhead(entry);
        if (decision == SpoolPolicy.Decision.CRITICAL && !journaled) {
            log.error("No se pudo escribir el evento critico {} en el spool antes de enviarlo; se envia igual",
                    event.eventId());
        }
        try {
            kafka.send(SpoolService.toRecord(entry, false)).whenComplete((result, error) -> {
                if (error == null) {
                    onSuccess(event, entry, journaled);
                } else {
                    onFailure(event, entry, decision, journaled, error);
                }
            });
        } catch (RuntimeException error) {
            // send() lanza excepcion directa si no obtiene metadatos del broker en max.block.ms
            onFailure(event, entry, decision, journaled, error);
        }
    }

    private void onSuccess(SimulatedEvent event, SpoolEntry entry, boolean journaled) {
        if (journaled) {
            spool.confirmed(entry);
        }
        lastAckAt = clock.instant();
        count(event.type(), "ok");
        if (failing.compareAndSet(true, false)) {
            log.warn("Kafka acepta envios de nuevo");
        }
    }

    private void onFailure(SimulatedEvent event, SpoolEntry entry, SpoolPolicy.Decision decision, boolean journaled,
                           Throwable error) {
        lastFailureAt = clock.instant();
        lastError = error.getClass().getSimpleName() + ": " + error.getMessage();
        if (failing.compareAndSet(false, true)) {
            log.warn("Kafka no disponible ({}). Criticos y telemetria util al spool; resumen cada 30 s", lastError);
        }
        switch (decision) {
            case CRITICAL -> {
                if (journaled) {
                    spool.sendFailed(entry, error);
                    criticalSpooled.incrementAndGet();
                    count(event.type(), "spooled");
                } else if (spool.storeCritical(entry)) {
                    criticalSpooled.incrementAndGet();
                    count(event.type(), "spooled");
                } else {
                    count(event.type(), "lost");
                    log.error("EVENTO CRITICO PERDIDO {}: Kafka y el spool en disco fallaron", event.eventId());
                }
            }
            case TELEMETRY -> {
                if (spool.storeTelemetry(entry)) {
                    telemetrySpooled.incrementAndGet();
                    count(event.type(), "spooled");
                } else {
                    dropped.incrementAndGet();
                    count(event.type(), "dropped");
                }
            }
            case DISCARD -> {
                dropped.incrementAndGet();
                count(event.type(), "dropped");
            }
        }
    }

    /** Un solo aviso por intervalo mientras Kafka falla, en vez de una linea por evento. */
    @Scheduled(fixedDelayString = "${simulator.failure-log-interval-ms:30000}")
    public void logFailureSummary() {
        long c = criticalSpooled.getAndSet(0);
        long t = telemetrySpooled.getAndSet(0);
        long d = dropped.getAndSet(0);
        if (failing.get() && (c + t + d) > 0) {
            log.warn("Kafka no disponible: en el ultimo intervalo {} criticos y {} de telemetria al spool, "
                    + "{} lecturas normales descartadas (no sirven para correlacionar)", c, t, d);
        }
    }

    public Map<String, Object> kafkaWriteStatus() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("kafkaAceptaEnvios", !failing.get());
        status.put("ultimoEnvioConfirmado", lastAckAt);
        status.put("ultimaFalla", lastFailureAt);
        status.put("ultimoError", lastError);
        return status;
    }

    private void count(SensorType type, String result) {
        metrics.counter("simulator.events.sent", "sensor_type", type.name(), "result", result).increment();
    }
}
