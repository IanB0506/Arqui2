package gt.umg.centinela.simulator.publish;

import gt.umg.centinela.common.json.Json;
import gt.umg.centinela.common.model.SensorType;
import gt.umg.centinela.simulator.generator.SimulatedEvent;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.EnumSet;
import java.util.Set;

/**
 * Publica eventos simulados en Kafka.
 *
 * <p>Politica de perdida aceptable (ADR-006): los botones de panico y los reportes ciudadanos son
 * eventos criticos con perdida cero; si Kafka no esta disponible se guardan en disco (spool) y se
 * reenvian despues. La telemetria de alto volumen (LPR, velocidad, acusticos) tolera perdida
 * durante una caida del broker: guardarla toda llenaria el disco y su valor caduca en minutos.
 */
@Component
public class EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(EventPublisher.class);
    private static final Set<SensorType> CRITICAL = EnumSet.of(SensorType.PANIC, SensorType.CITIZEN_REPORT);

    private final KafkaTemplate<String, String> kafka;
    private final SpoolService spool;
    private final MeterRegistry metrics;

    public EventPublisher(KafkaTemplate<String, String> kafka, SpoolService spool, MeterRegistry metrics) {
        this.kafka = kafka;
        this.spool = spool;
        this.metrics = metrics;
    }

    public void publish(SimulatedEvent event) {
        String topic = event.type().rawTopic();
        String json = Json.write(event.payload());
        try {
            kafka.send(topic, event.key(), json).whenComplete((result, error) -> {
                if (error == null) {
                    count(event.type(), "ok");
                } else {
                    onFailure(event, topic, json, error);
                }
            });
        } catch (RuntimeException error) {
            // send() lanza excepcion directa si no obtiene metadatos del broker en max.block.ms
            onFailure(event, topic, json, error);
        }
    }

    private void onFailure(SimulatedEvent event, String topic, String json, Throwable error) {
        if (CRITICAL.contains(event.type())) {
            if (spool.store(topic, event.key(), json)) {
                count(event.type(), "spooled");
                log.warn("Kafka no disponible, evento critico {} guardado en spool: {}", event.eventId(), error.getMessage());
            } else {
                count(event.type(), "lost");
                log.error("EVENTO CRITICO PERDIDO {}: Kafka y el spool en disco fallaron", event.eventId());
            }
        } else {
            count(event.type(), "dropped");
            log.debug("Evento de telemetria descartado por falla de Kafka: {}", error.getMessage());
        }
    }

    private void count(SensorType type, String result) {
        metrics.counter("simulator.events.sent", "sensor_type", type.name(), "result", result).increment();
    }
}
