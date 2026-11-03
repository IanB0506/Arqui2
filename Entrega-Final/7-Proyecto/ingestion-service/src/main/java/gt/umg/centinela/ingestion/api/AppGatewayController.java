package gt.umg.centinela.ingestion.api;

import gt.umg.centinela.common.json.InvalidEventException;
import gt.umg.centinela.common.json.Json;
import gt.umg.centinela.common.model.SensorType;
import gt.umg.centinela.common.model.raw.CitizenReportEvent;
import gt.umg.centinela.common.model.raw.PanicButtonEvent;
import gt.umg.centinela.ingestion.pipeline.EventNormalizer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1")
public class AppGatewayController {
    private final GatewaySpool gateway;
    private final EventNormalizer normalizer;

    public AppGatewayController(GatewaySpool gateway, EventNormalizer normalizer) {
        this.gateway = gateway;
        this.normalizer = normalizer;
    }

    @PostMapping("/panic")
    public ResponseEntity<Map<String, Object>> panic(@RequestBody PanicButtonEvent body) {
        PanicButtonEvent event = new PanicButtonEvent(
                body.eventId() != null ? body.eventId() : UUID.randomUUID(),
                body.timestamp() != null ? body.timestamp() : Instant.now(),
                body.zone(), body.alertType(), body.deviceId(), body.userContext());
        String json = Json.write(event);
        normalizer.parse(json, PanicButtonEvent.class);
        return respond(event.eventId(), gateway.submit(SensorType.PANIC.rawTopic(), event.eventId().toString(),
                event.deviceId(), json));
    }

    @PostMapping("/citizen-reports")
    public ResponseEntity<Map<String, Object>> citizenReport(@RequestBody CitizenReportEvent body) {
        CitizenReportEvent event = new CitizenReportEvent(
                body.eventId() != null ? body.eventId() : UUID.randomUUID(),
                body.timestamp() != null ? body.timestamp() : Instant.now(),
                body.eventType(), body.message(), body.location(), body.origin(), body.reporterId());
        String json = Json.write(event);
        normalizer.parse(json, CitizenReportEvent.class);
        String key = event.reporterId() != null ? event.reporterId() : event.eventId().toString();
        return respond(event.eventId(), gateway.submit(SensorType.CITIZEN_REPORT.rawTopic(), event.eventId().toString(),
                key, json));
    }

    @GetMapping("/gateway/status")
    public Map<String, Object> status() {
        return gateway.status();
    }

    private static ResponseEntity<Map<String, Object>> respond(UUID eventId, GatewaySpool.Outcome outcome) {
        return switch (outcome) {
            case PUBLICADO -> ResponseEntity.status(HttpStatus.ACCEPTED).body(Map.of("event_id", eventId, "estado", "RECIBIDO"));
            case EN_COLA -> ResponseEntity.status(HttpStatus.ACCEPTED).body(Map.of("event_id", eventId, "estado", "EN_COLA"));
            case RECHAZADO -> ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).header(HttpHeaders.RETRY_AFTER, "10")
                    .body(Map.of("event_id", eventId, "estado", "NO_DISPONIBLE",
                            "error", "No se pudo guardar el evento; reintente con el mismo event_id"));
        };
    }

    @ExceptionHandler(InvalidEventException.class)
    public ResponseEntity<Map<String, String>> invalid(InvalidEventException e) {
        return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
    }
}
