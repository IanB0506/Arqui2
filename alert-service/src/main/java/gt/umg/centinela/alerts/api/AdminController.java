package gt.umg.centinela.alerts.api;

import gt.umg.centinela.alerts.dlq.DlqReprocessor;
import gt.umg.centinela.alerts.jobs.AlertExpiryJob;
import gt.umg.centinela.alerts.jobs.PlateRetentionJob;
import gt.umg.centinela.alerts.jobs.RuleQualityJob;
import gt.umg.centinela.alerts.jobs.SloMonitorJob;
import gt.umg.centinela.alerts.jobs.WatchlistSyncJob;
import gt.umg.centinela.alerts.persistence.AuditRepository;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Operaciones de administracion (rol ADMIN, quedan en la bitacora de auditoria):
 * <ul>
 *   <li>{@code POST /api/admin/jobs/rule-quality | slo | plate-retention | alert-expiry | watchlist}:
 *       ejecuta una tarea periodica sin esperar a su horario.</li>
 *   <li>{@code GET /api/admin/dlq}: resumen de la cola de mensajes muertos; {@code POST /api/admin/dlq/reprocess}
 *       la reinyecta (por defecto solo lo que fallo por errores transitorios o desconocidos).</li>
 *   <li>{@code POST /api/admin/correlation/reset}: limpia el estado temporal del motor de correlacion
 *       (ventanas, incidentes abiertos, anclas pendientes, sospechosos). Sirve para que la validacion de
 *       la demo sea reproducible. NO borra eventos ni alertas historicas de la base de datos.</li>
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

    public AdminController(RuleQualityJob ruleQuality, SloMonitorJob slo, PlateRetentionJob retention,
                           AlertExpiryJob expiry, WatchlistSyncJob watchlist, StringRedisTemplate redis,
                           AuditRepository audit, DlqReprocessor dlq) {
        this.ruleQuality = ruleQuality;
        this.slo = slo;
        this.retention = retention;
        this.expiry = expiry;
        this.watchlist = watchlist;
        this.redis = redis;
        this.audit = audit;
        this.dlq = dlq;
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

    @PostMapping("/correlation/reset")
    public Map<String, Object> resetCorrelationState(Authentication auth) {
        Map<String, Object> deleted = new LinkedHashMap<>();
        long total = 0;
        for (String pattern : CORRELATION_KEYS) {
            List<String> keys = new ArrayList<>();
            try (Cursor<String> cursor = redis.scan(ScanOptions.scanOptions().match(pattern).count(1000).build())) {
                cursor.forEachRemaining(keys::add);
            }
            long count = keys.isEmpty() ? 0 : redis.delete(keys);
            deleted.put(pattern, count);
            total += count;
        }
        audit.record(auth.getName(), "REINICIAR_ESTADO_CORRELACION", "redis", deleted);
        return Map.of("deletedKeys", total, "detail", deleted, "executedAt", Instant.now().toString());
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
