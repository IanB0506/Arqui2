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
            consumer.currentLag(partition).ifPresent(engine::noteConsumerLag);
            engine.onEvent(event, publisher::publish);
        } catch (RuntimeException e) {
            if (!FailureClassifier.isPermanent(e)) {
                engine.noteProcessingFailure(partition.toString());
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

    @Scheduled(fixedDelay = 2000, initialDelay = 5000)
    public void sweepPending() {
        engine.sweepPending(publisher::publish);
    }
}
