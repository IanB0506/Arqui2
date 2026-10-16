package gt.umg.centinela.alerts.kafka;

import gt.umg.centinela.alerts.persistence.EventRepository;
import gt.umg.centinela.common.Topics;
import gt.umg.centinela.common.json.Json;
import gt.umg.centinela.common.model.NormalizedEvent;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Guarda en TimescaleDB todos los eventos normalizados (grupo "event-store"). Es un consumidor
 * independiente del motor de correlacion: si la base de datos se pone lenta, la correlacion en
 * tiempo real no se entera (cada grupo de consumidores avanza a su propio ritmo).
 */
@Component
public class EventStoreListener {

    private static final Logger log = LoggerFactory.getLogger(EventStoreListener.class);

    private final EventRepository repository;
    private final MeterRegistry metrics;

    public EventStoreListener(EventRepository repository, MeterRegistry metrics) {
        this.repository = repository;
        this.metrics = metrics;
    }

    @KafkaListener(topics = Topics.NORMALIZED, groupId = "event-store", containerFactory = "batchFactory",
            concurrency = "${alerts.event-store-concurrency:2}")
    public void store(List<ConsumerRecord<String, String>> records) {
        List<NormalizedEvent> events = new ArrayList<>(records.size());
        for (ConsumerRecord<String, String> record : records) {
            try {
                events.add(Json.read(record.value(), NormalizedEvent.class));
            } catch (RuntimeException e) {
                metrics.counter("alerts.events.corrupt").increment();
                log.warn("Evento normalizado corrupto en offset {}: {}", record.offset(), e.getMessage());
            }
        }
        int inserted = repository.saveAll(events);
        metrics.counter("alerts.events.stored").increment(inserted);
    }
}
