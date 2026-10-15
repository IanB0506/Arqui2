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

/**
 * Puerta de entrada HTTP para la app movil ciudadana (boton de panico y reportes).
 *
 * <p>Los sensores fijos publican directo en Kafka, pero una app en un celular no deberia hablar con
 * Kafka: se expone un endpoint REST que valida el contrato y lo deja en el mismo topic crudo, de modo
 * que siga exactamente el mismo flujo que el resto. Este endpoint es el que se estresa con JMeter.
 *
 * <p>Respuestas: 202 {@code RECIBIDO} (Kafka lo confirmo), 202 {@code EN_COLA} (Kafka no respondio y
 * el evento quedo en el spool en disco: se reenvia solo, ADR-006), 503 {@code NO_DISPONIBLE} (ni Kafka
 * ni el disco: la app debe reintentar con el mismo event_id) y 400 si el contrato es invalido.
 */
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

    /** Estado del spool del gateway (lo consultan el vigilante y centinela.ps1). */
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
