package gt.umg.centinela.alerts.api;

import gt.umg.centinela.alerts.config.AlertServiceProperties;
import gt.umg.centinela.alerts.live.AlertBroadcaster;
import gt.umg.centinela.alerts.persistence.AlertRepository;
import gt.umg.centinela.alerts.persistence.AlertView;
import gt.umg.centinela.alerts.persistence.AuditRepository;
import gt.umg.centinela.alerts.persistence.EventRepository;
import gt.umg.centinela.common.geo.Zone;
import gt.umg.centinela.common.geo.ZoneCatalog;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * API de alertas para la consola del operador y para la validacion automatica del simulador.
 * <pre>
 *   GET   /api/alerts?since=...&amp;zoneId=4&amp;type=ROBO_EN_CURSO&amp;status=NEW
 *   GET   /api/alerts/{id}
 *   PATCH /api/alerts/{id}/validation   {"resolution":"FALSE_POSITIVE","notes":"..."}
 *   GET   /api/alerts/stream            (Server-Sent Events)
 *   GET   /api/events/{id}?at=...       (evento original de una evidencia: trazabilidad alerta -> sensor)
 *   GET   /api/stats/summary
 *   GET   /api/zones
 * </pre>
 */
@RestController
@RequestMapping("/api")
public class AlertController {

    private final AlertRepository alerts;
    private final AuditRepository audit;
    private final AlertBroadcaster broadcaster;
    private final EventRepository events;
    private final Duration sloWindow;

    public AlertController(AlertRepository alerts, AuditRepository audit, AlertBroadcaster broadcaster,
                           EventRepository events, AlertServiceProperties properties) {
        this.alerts = alerts;
        this.audit = audit;
        this.broadcaster = broadcaster;
        this.events = events;
        this.sloWindow = properties.slo().window();
    }

    public record ValidationRequest(
            @NotNull @Pattern(regexp = "CONFIRMED|FALSE_POSITIVE") String resolution,
            @Size(max = 500) String notes) {
    }

    @GetMapping("/alerts")
    public List<AlertView> list(@RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant since,
                                @RequestParam(required = false) Integer zoneId,
                                @RequestParam(required = false) String type,
                                @RequestParam(required = false) String status,
                                @RequestParam(defaultValue = "200") int limit) {
        Instant from = since != null ? since : Instant.now().minus(Duration.ofHours(24));
        return alerts.find(from, zoneId, type, status, Math.min(Math.max(limit, 1), 1000));
    }

    @GetMapping("/alerts/{id}")
    public ResponseEntity<AlertView> get(@PathVariable UUID id) {
        return ResponseEntity.of(alerts.findById(id));
    }

    /**
     * Evento guardado tal como lo vio el sistema (sin seudonimos internos). {@code at} es la hora del evento
     * que trae la evidencia: acota la busqueda a su fragmento de la hypertable.
     */
    @GetMapping("/events/{id}")
    public ResponseEntity<Map<String, Object>> event(@PathVariable UUID id,
                                                     @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant at) {
        return ResponseEntity.of(events.find(id, at));
    }

    @PatchMapping("/alerts/{id}/validation")
    public ResponseEntity<AlertView> validate(@PathVariable UUID id, @Valid @RequestBody ValidationRequest request,
                                              Authentication auth) {
        if (!alerts.validate(id, request.resolution(), auth.getName(), request.notes(), Instant.now())) {
            return ResponseEntity.notFound().build();
        }
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("resolution", request.resolution());
        detail.put("notes", request.notes());
        audit.record(auth.getName(), "VALIDAR_ALERTA", id.toString(), detail);
        AlertView updated = alerts.findById(id).orElseThrow();
        broadcaster.broadcast(updated);
        return ResponseEntity.ok(updated);
    }

    @GetMapping(path = "/alerts/stream", produces = "text/event-stream")
    public SseEmitter stream() {
        return broadcaster.subscribe();
    }

    @GetMapping("/stats/summary")
    public Map<String, Object> summary(@RequestParam(defaultValue = "60") int minutes) {
        Instant now = Instant.now();
        // Los conteos usan la ventana pedida; la latencia, la del SLO-1 (5 min): asi una caida ya superada
        // no deja la p95 alta durante una hora
        Map<String, Object> result = new LinkedHashMap<>(
                alerts.summary(now.minus(Duration.ofMinutes(minutes)), now.minus(sloWindow)));
        result.put("windowMinutes", minutes);
        result.put("latencyWindowMinutes", sloWindow.toMinutes());
        result.put("connectedConsoles", broadcaster.connected());
        return result;
    }

    @GetMapping("/zones")
    public List<Zone> zones() {
        return ZoneCatalog.all();
    }
}
