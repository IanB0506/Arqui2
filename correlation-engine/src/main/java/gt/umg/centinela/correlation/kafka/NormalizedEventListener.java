package gt.umg.centinela.correlation.kafka;

import gt.umg.centinela.common.Topics;
import gt.umg.centinela.common.json.Json;
import gt.umg.centinela.common.model.NormalizedEvent;
import gt.umg.centinela.correlation.engine.CorrelationEngine;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Consume eventos normalizados (grupo "correlation"). La llave del mensaje es la zona, por lo que
 * todos los eventos de una zona llegan en orden a la misma instancia.
 */
@Component
public class NormalizedEventListener {

    private static final Logger log = LoggerFactory.getLogger(NormalizedEventListener.class);

    private final CorrelationEngine engine;
    private final AlertPublisher publisher;
    private final MeterRegistry metrics;

    public NormalizedEventListener(CorrelationEngine engine, AlertPublisher publisher, MeterRegistry metrics) {
        this.engine = engine;
        this.publisher = publisher;
        this.metrics = metrics;
    }

    @KafkaListener(topics = Topics.NORMALIZED, groupId = "correlation", concurrency = "${correlation.concurrency:6}")
    public void onEvent(ConsumerRecord<String, String> record) throws Exception {
        NormalizedEvent event = Json.read(record.value(), NormalizedEvent.class);
        publisher.publish(engine.onEvent(event));
        metrics.counter("correlation.events.processed", "sensor_type", event.sensorType().name()).increment();
    }

    /** Cada 2 s revisa anclas cuyo plazo de corroboracion vencio (falsos positivos). */
    @Scheduled(fixedDelay = 2000, initialDelay = 5000)
    public void sweepPending() {
        try {
            publisher.publish(engine.sweepPending());
        } catch (Exception e) {
            log.warn("No se pudo revisar anclas pendientes: {}", e.getMessage());
        }
    }
}
