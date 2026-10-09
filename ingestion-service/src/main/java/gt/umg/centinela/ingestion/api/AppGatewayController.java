package gt.umg.centinela.ingestion.api;

import gt.umg.centinela.common.json.InvalidEventException;
import gt.umg.centinela.common.json.Json;
import gt.umg.centinela.common.model.SensorType;
import gt.umg.centinela.common.model.raw.CitizenReportEvent;
import gt.umg.centinela.common.model.raw.PanicButtonEvent;
import gt.umg.centinela.ingestion.pipeline.EventNormalizer;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Puerta de entrada HTTP para la app movil ciudadana (boton de panico y reportes).
 *
 * <p>Los sensores fijos publican directo en Kafka, pero una app en un celular no deberia hablar con
 * Kafka: se expone un endpoint REST que valida el contrato y lo deja en el mismo topic crudo, de modo
 * que siga exactamente el mismo flujo que el resto. Este endpoint es el que se estresa con JMeter.
 */
@RestController
@RequestMapping("/api/v1")
public class AppGatewayController {

    private final KafkaTemplate<String, String> kafka;
    private final EventNormalizer normalizer;

    public AppGatewayController(KafkaTemplate<String, String> kafka, EventNormalizer normalizer) {
        this.kafka = kafka;
        this.normalizer = normalizer;
    }

    @PostMapping("/panic")
    public ResponseEntity<Map<String, Object>> panic(@RequestBody PanicButtonEvent body) throws Exception {
        PanicButtonEvent event = new PanicButtonEvent(
                body.eventId() != null ? body.eventId() : UUID.randomUUID(),
                body.timestamp() != null ? body.timestamp() : Instant.now(),
                body.zone(), body.alertType(), body.deviceId(), body.userContext());
        String json = Json.write(event);
        normalizer.parse(json, PanicButtonEvent.class);
        kafka.send(SensorType.PANIC.rawTopic(), event.deviceId(), json).get(5, TimeUnit.SECONDS);
        return accepted(event.eventId());
    }

    @PostMapping("/citizen-reports")
    public ResponseEntity<Map<String, Object>> citizenReport(@RequestBody CitizenReportEvent body) throws Exception {
        CitizenReportEvent event = new CitizenReportEvent(
                body.eventId() != null ? body.eventId() : UUID.randomUUID(),
                body.timestamp() != null ? body.timestamp() : Instant.now(),
                body.eventType(), body.message(), body.location(), body.origin(), body.reporterId());
        String json = Json.write(event);
        normalizer.parse(json, CitizenReportEvent.class);
        String key = event.reporterId() != null ? event.reporterId() : event.eventId().toString();
        kafka.send(SensorType.CITIZEN_REPORT.rawTopic(), key, json).get(5, TimeUnit.SECONDS);
        return accepted(event.eventId());
    }

    private static ResponseEntity<Map<String, Object>> accepted(UUID eventId) {
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(Map.of("event_id", eventId, "estado", "RECIBIDO"));
    }

    @ExceptionHandler(InvalidEventException.class)
    public ResponseEntity<Map<String, String>> invalid(InvalidEventException e) {
        return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
    }
}
