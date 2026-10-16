package gt.umg.centinela.simulator.api;

import gt.umg.centinela.simulator.publish.EventPublisher;
import gt.umg.centinela.simulator.publish.SpoolService;
import gt.umg.centinela.simulator.watchdog.NodeWatchdog;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Vigilante y spool del nodo generador:
 * <ul>
 *   <li>GET  /api/watchdog: salud del nodo B, Kafka, grupos de consumidores, spool (lo usa la pagina
 *       /vigilante.html y centinela.ps1).</li>
 *   <li>GET  /api/simulator/spool, POST /api/simulator/spool/resend (reenviar ya) y
 *       POST /api/simulator/spool/rejected/requeue?spool=critical|telemetry.</li>
 * </ul>
 */
@RestController
@RequestMapping("/api")
public class WatchdogController {

    private final NodeWatchdog watchdog;
    private final SpoolService spool;
    private final EventPublisher publisher;

    public WatchdogController(NodeWatchdog watchdog, SpoolService spool, EventPublisher publisher) {
        this.watchdog = watchdog;
        this.spool = spool;
        this.publisher = publisher;
    }

    @GetMapping("/watchdog")
    public Map<String, Object> watchdog() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("watchdog", watchdog.snapshot());
        result.put("spool", spool.status());
        result.put("publisher", publisher.kafkaWriteStatus());
        return result;
    }

    @GetMapping("/simulator/spool")
    public Map<String, Object> spool() {
        return spool.status();
    }

    @PostMapping("/simulator/spool/resend")
    public Map<String, Object> resend() {
        spool.resend();
        return spool.status();
    }

    @PostMapping("/simulator/spool/rejected/requeue")
    public Map<String, Object> requeue(@RequestParam(defaultValue = "critical") String spool) throws IOException {
        return Map.of("reencolados", this.spool.requeueRejected(spool));
    }
}
