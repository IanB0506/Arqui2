package gt.umg.centinela.alerts.kafka;

import gt.umg.centinela.alerts.live.AlertBroadcaster;
import gt.umg.centinela.alerts.persistence.AlertRepository;
import gt.umg.centinela.common.Topics;
import gt.umg.centinela.common.json.Json;
import gt.umg.centinela.common.model.AlertEvent;
import gt.umg.centinela.common.model.AlertStatus;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

/**
 * Persiste las alertas, mide la latencia extremo a extremo (evento del sensor -> alerta guardada y
 * visible) y las retransmite a las consolas conectadas.
 */
@Component
public class AlertStoreListener {

    private final AlertRepository repository;
    private final AlertBroadcaster broadcaster;
    private final Timer endToEnd;

    public AlertStoreListener(AlertRepository repository, AlertBroadcaster broadcaster, MeterRegistry metrics) {
        this.repository = repository;
        this.broadcaster = broadcaster;
        this.endToEnd = Timer.builder("alerts.end_to_end.latency")
                .description("Evento del sensor -> alerta persistida (SLO: p95 < 2 s)")
                .serviceLevelObjectives(Duration.ofMillis(250), Duration.ofMillis(500), Duration.ofSeconds(1),
                        Duration.ofSeconds(2), Duration.ofSeconds(5))
                .publishPercentileHistogram()
                .register(metrics);
    }

    @KafkaListener(topics = Topics.ALERTS, groupId = "alert-store")
    public void onAlert(ConsumerRecord<String, String> record) {
        AlertEvent alert = Json.read(record.value(), AlertEvent.class);
        long latency = repository.save(alert, Instant.now());
        if (alert.status() == AlertStatus.NEW || alert.status() == AlertStatus.ESCALATED) {
            endToEnd.record(Duration.ofMillis(latency));
        }
        repository.findById(alert.alertId()).ifPresent(broadcaster::broadcast);
    }
}
