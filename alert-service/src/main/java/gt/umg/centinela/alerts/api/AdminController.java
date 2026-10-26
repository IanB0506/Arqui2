package gt.umg.centinela.alerts.api;

import gt.umg.centinela.alerts.dlq.DlqReprocessor;
import gt.umg.centinela.alerts.jobs.AlertExpiryJob;
import gt.umg.centinela.alerts.jobs.PlateRetentionJob;
import gt.umg.centinela.alerts.jobs.RuleQualityJob;
import gt.umg.centinela.alerts.jobs.SloMonitorJob;
import gt.umg.centinela.alerts.jobs.WatchlistSyncJob;
import gt.umg.centinela.alerts.live.SystemAlerts;
import gt.umg.centinela.alerts.persistence.AuditRepository;
import gt.umg.centinela.common.geo.ZoneCatalog;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Operaciones de administracion (rol ADMIN, quedan en la bitacora de auditoria):
 * <ul>
 *   <li>{@code POST /api/admin/jobs/rule-quality | slo | plate-retention | alert-expiry | watchlist}:
 *       ejecuta una tarea periodica sin esperar a su horario.</li>
 *   <li>{@code GET /api/admin/dlq}: resumen de la cola de mensajes muertos; {@code POST /api/admin/dlq/reprocess}
 *       la reinyecta (por defecto solo lo que fallo por errores transitorios o desconocidos).</li>
 *   <li>{@code POST /api/admin/slo/lost-critical-events}: un simulador informa eventos criticos que no pudo
 *       guardar (SLO-2 incumplido): se genera una alerta de sistema CRITICAL en la consola.</li>
 *   <li>{@code POST /api/admin/correlation/reset}: limpia el estado temporal del motor de correlacion
 *       (ventanas, incidentes abiertos, anclas pendientes, sospechosos). Sirve para que la validacion de
 *       la demo sea reproducible. Con {@code ?zonas=4,10} solo limpia el historial reciente y los incidentes
 *       abiertos de esas zonas.
 *       NO borra eventos ni alertas historicas de la base de datos.</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/admin")
public class AdminController {

    private static final List<String> CORRELATION_KEYS = List.of(
            "win:*", "incident:*", "pending:*", "corroborated:*", "suspect:*", "suspect-zone:*",
            "devpanic:*", "devflag:*");

    private final RuleQualityJob ruleQuality;
    private final SloMonitorJob slo;
    private final PlateRetentionJob retention;
    private final AlertExpiryJob expiry;
    private final WatchlistSyncJob watchlist;
    private final StringRedisTemplate redis;
    private final AuditRepository audit;
    private final DlqReprocessor dlq;
    private final SystemAlerts systemAlerts;

    public AdminController(RuleQualityJob ruleQuality, SloMonitorJob slo, PlateRetentionJob retention,
                           AlertExpiryJob expiry, WatchlistSyncJob watchlist, StringRedisTemplate redis,
                           AuditRepository audit, DlqReprocessor dlq, SystemAlerts systemAlerts) {
        this.ruleQuality = ruleQuality;
        this.slo = slo;
        this.retention = retention;
        this.expiry = expiry;
        this.watchlist = watchlist;
        this.redis = redis;
        this.audit = audit;
        this.dlq = dlq;
        this.systemAlerts = systemAlerts;
    }

    /** Lo que informa un simulador al recuperar la conexion: cuantos eventos criticos no pudo guardar. */
    public record LostCriticalEvents(@NotBlank @Size(max = 40) String node, @Min(1) long lost) {
    }

    @PostMapping("/slo/lost-critical-events")
    public Map<String, Object> lostCriticalEvents(@Valid @RequestBody LostCriticalEvents report, Authentication auth) {
        String description = String.format(Locale.ROOT,
                "El simulador del %s no pudo guardar %d evento(s) critico(s) (panico, reporte ciudadano o disparo): "
                        + "Kafka no respondia y su spool en disco estaba lleno o fallo. SLO-2 incumplido (objetivo: 0). "
                        + "Revisar el disco de esa PC y por que el nodo A estuvo caido tanto tiempo; el log del "
                        + "simulador (EVENTO CRITICO PERDIDO) tiene los identificadores.", report.node(), report.lost());
        systemAlerts.notify("CRITICAL", "Eventos criticos perdidos", description, Instant.now());
        audit.record(auth.getName(), "SLO2_EVENTOS_PERDIDOS", report.node(), Map.of("perdidos", report.lost()));
        return Map.of("registrado", true, "perdidos", report.lost());
    }

    @GetMapping("/dlq")
    public Map<String, Object> dlqSummary(@RequestParam(defaultValue = "20") int ultimos) {
        return dlq.summary(Math.min(ultimos, 200));
    }

    @PostMapping("/dlq/reprocess")
    public Map<String, Object> dlqReprocess(@RequestParam(defaultValue = "false") boolean incluirPermanentes,
                                            Authentication auth) {
        DlqReprocessor.Result r = dlq.reprocess(incluirPermanentes, incluirPermanentes);
        Map<String, Object> detail = Map.of("reprocessed", r.reprocessed(), "skipped", r.skipped(), "read", r.read(),
                "incluirPermanentes", incluirPermanentes);
        audit.record(auth.getName(), "REPROCESAR_DLQ", "kafka", detail);
        return detail;
    }

    /**
     * Sin {@code zonas} limpia todo el estado temporal. Con {@code zonas=4,10} solo el de esas zonas: historial
     * reciente de eventos (ventanas) e incidentes abiertos. Lo usa el simulador al lanzar un escenario, para que
     * el ruido de fondo previo no lo corrobore y su alerta salga como nueva y no como revision de otra.
     */
    @PostMapping("/correlation/reset")
    public ResponseEntity<Map<String, Object>> resetCorrelationState(@RequestParam(required = false) List<Integer> zonas,
                                                                     Authentication auth) {
        if (zonas != null && (zonas.isEmpty() || zonas.stream().anyMatch(z -> z == null || z < 1 || z > ZoneCatalog.all().size()))) {
            return ResponseEntity.badRequest().body(Map.of("error", "Zonas validas: 1 a " + ZoneCatalog.all().size()));
        }
        List<String> patterns = zonas == null ? CORRELATION_KEYS
                : zonas.stream().distinct().flatMap(z -> java.util.stream.Stream.of("win:" + z + ":*", "incident:*:" + z)).toList();
        Map<String, Object> deleted = new LinkedHashMap<>();
        long total = 0;
        for (String pattern : patterns) {
            List<String> keys = new ArrayList<>();
            try (Cursor<String> cursor = redis.scan(ScanOptions.scanOptions().match(pattern).count(1000).build())) {
                cursor.forEachRemaining(keys::add);
            }
            long count = keys.isEmpty() ? 0 : redis.delete(keys);
            deleted.put(pattern, count);
            total += count;
        }
        audit.record(auth.getName(), zonas == null ? "REINICIAR_ESTADO_CORRELACION" : "REINICIAR_ZONAS", "redis", deleted);
        return ResponseEntity.ok(Map.of("deletedKeys", total, "detail", deleted, "executedAt", Instant.now().toString()));
    }

    @PostMapping("/jobs/{job}")
    public ResponseEntity<Map<String, Object>> run(@PathVariable String job) {
        switch (job) {
            case "rule-quality" -> ruleQuality.compute();
            case "slo" -> slo.measure();
            case "plate-retention" -> retention.purge();
            case "alert-expiry" -> expiry.expire();
            case "watchlist" -> watchlist.sync();
            default -> {
                return ResponseEntity.badRequest().body(Map.of("error", "Tarea desconocida: " + job));
            }
        }
        return ResponseEntity.ok(Map.of("job", job, "executedAt", Instant.now().toString()));
    }
}
