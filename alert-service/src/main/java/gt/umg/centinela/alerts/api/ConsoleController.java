package gt.umg.centinela.alerts.api;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/** Datos que la consola del operador necesita para armar sus enlaces. */
@RestController
@RequestMapping("/api/console")
public class ConsoleController {

    private final int grafanaPort;

    public ConsoleController(@Value("${alerts.console.grafana-port:3000}") int grafanaPort) {
        this.grafanaPort = grafanaPort;
    }

    @GetMapping("/config")
    public Map<String, Object> config() {
        return Map.of("grafanaPort", grafanaPort);
    }
}
