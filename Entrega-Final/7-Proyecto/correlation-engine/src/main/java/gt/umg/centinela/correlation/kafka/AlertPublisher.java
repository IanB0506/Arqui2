package gt.umg.centinela.correlation.kafka;

import gt.umg.centinela.common.Topics;
import gt.umg.centinela.common.json.Json;
import gt.umg.centinela.common.model.AlertEvent;
import gt.umg.centinela.common.model.AlertStatus;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
public class AlertPublisher {
    private final KafkaTemplate<String, String> kafka;
    private final MeterRegistry metrics;
    private final Timer detectionLatency;

    public AlertPublisher(KafkaTemplate<String, String> kafka, MeterRegistry metrics) {
        this.kafka = kafka;
        this.metrics = metrics;
        this.detectionLatency = Timer.builder("correlation.detection.latency")
                .description("Tiempo entre el evento que completa la correlacion y la generacion de la alerta")
                .serviceLevelObjectives(Duration.ofMillis(250), Duration.ofMillis(500), Duration.ofSeconds(1),
                        Duration.ofSeconds(2), Duration.ofSeconds(5))
                .publishPercentileHistogram()
                .register(metrics);
    }

    public void publish(AlertEvent alert) {
        try {
            // Envío síncrono: si Kafka no confirma, el motor no guarda el incidente y el evento se reintenta.
            kafka.send(Topics.ALERTS, String.valueOf(alert.zoneId()), Json.write(alert)).get(20, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrumpido publicando la alerta " + alert.alertId(), e);
        } catch (ExecutionException | TimeoutException | RuntimeException e) {
            throw new IllegalStateException("No se pudo publicar la alerta " + alert.alertId(), e);
        }
        metrics.counter("correlation.alerts", "alert_type", alert.alertType(), "severity", alert.severity().name(),
                "status", alert.status().name()).increment();
        if (alert.status() == AlertStatus.NEW || alert.status() == AlertStatus.ESCALATED) {
            detectionLatency.record(Duration.between(alert.triggerEventTime(), alert.createdAt()));
        }
    }
}
