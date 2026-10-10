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
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Publica alertas en el topic {@code alerts} y mide la latencia de deteccion. */
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

    public void publish(List<AlertEvent> alerts) throws Exception {
        for (AlertEvent alert : alerts) {
            kafka.send(Topics.ALERTS, String.valueOf(alert.zoneId()), Json.write(alert)).get(10, TimeUnit.SECONDS);
            metrics.counter("correlation.alerts", "alert_type", alert.alertType(), "severity", alert.severity().name(),
                    "status", alert.status().name()).increment();
            // SLO: solo el primer aviso y los agravamientos (lo que el operador necesita saber ya)
            if (alert.status() == AlertStatus.NEW || alert.status() == AlertStatus.ESCALATED) {
                detectionLatency.record(Duration.between(alert.triggerEventTime(), alert.createdAt()));
            }
        }
    }
}
