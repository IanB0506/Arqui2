package gt.umg.centinela.correlation.kafka;

import gt.umg.centinela.common.Topics;
import gt.umg.centinela.common.json.Json;
import gt.umg.centinela.common.kafka.FailureClassifier;
import gt.umg.centinela.common.model.NormalizedEvent;
import gt.umg.centinela.correlation.engine.CorrelationEngine;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Consume eventos normalizados (grupo "correlation"). La llave del mensaje es la zona, por lo que
 * todos los eventos de una zona llegan en orden a la misma instancia.
 */
@Component
public class NormalizedEventListener implements ConsumerSeekAware {

    private final CorrelationEngine engine;
    private final AlertPublisher publisher;
    private final MeterRegistry metrics;

    public NormalizedEventListener(CorrelationEngine engine, AlertPublisher publisher, MeterRegistry metrics) {
        this.engine = engine;
        this.publisher = publisher;
        this.metrics = metrics;
    }

    @KafkaListener(topics = Topics.NORMALIZED, groupId = "correlation", concurrency = "${correlation.concurrency:6}")
    public void onEvent(ConsumerRecord<String, String> record, Consumer<?, ?> consumer) {
        TopicPartition partition = new TopicPartition(record.topic(), record.partition());
        NormalizedEvent event = Json.read(record.value(), NormalizedEvent.class);
        try {
            // Cola pendiente de la particion segun el ultimo fetch (sin llamadas extra al broker ni relojes)
            consumer.currentLag(partition).ifPresent(engine::noteConsumerLag);
            engine.onEvent(event, publisher::publish); // si Redis o Kafka fallan, se lanza y el evento se reintenta
        } catch (RuntimeException e) {
            if (!FailureClassifier.isPermanent(e)) {
                engine.noteProcessingFailure(partition.toString()); // los eventos de la particion esperan en Kafka
            }
            throw e;
        }
        engine.noteProcessingOk(partition.toString());
        metrics.counter("correlation.events.processed", "sensor_type", event.sensorType().name()).increment();
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        engine.notePartitionsAssigned();
    }

    /**
     * Cada 2 s revisa anclas cuyo plazo de corroboracion vencio (falsos positivos). Si Redis o Kafka no
     * responden, la falla la resume ScheduledTaskErrorHandler (una linea por minuto) y la ronda se repite.
     */
    @Scheduled(fixedDelay = 2000, initialDelay = 5000)
    public void sweepPending() {
        engine.sweepPending(publisher::publish);
    }
}
