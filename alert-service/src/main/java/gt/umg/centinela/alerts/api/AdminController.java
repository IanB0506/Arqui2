package gt.umg.centinela.alerts.api;

import gt.umg.centinela.alerts.jobs.AlertExpiryJob;
import gt.umg.centinela.alerts.jobs.PlateRetentionJob;
import gt.umg.centinela.alerts.jobs.RuleQualityJob;
import gt.umg.centinela.alerts.jobs.SloMonitorJob;
import gt.umg.centinela.alerts.jobs.WatchlistSyncJob;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.Map;

/**
 * Ejecucion manual de tareas periodicas (util en la demo para no esperar al horario programado):
 * {@code POST /api/admin/jobs/rule-quality | slo | plate-retention | alert-expiry | watchlist}.
 */
@RestController
@RequestMapping("/api/admin/jobs")
public class AdminController {

    private final RuleQualityJob ruleQuality;
    private final SloMonitorJob slo;
    private final PlateRetentionJob retention;
    private final AlertExpiryJob expiry;
    private final WatchlistSyncJob watchlist;

    public AdminController(RuleQualityJob ruleQuality, SloMonitorJob slo, PlateRetentionJob retention,
                           AlertExpiryJob expiry, WatchlistSyncJob watchlist) {
        this.ruleQuality = ruleQuality;
        this.slo = slo;
        this.retention = retention;
        this.expiry = expiry;
        this.watchlist = watchlist;
    }

    @PostMapping("/{job}")
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
