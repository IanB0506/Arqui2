package gt.umg.centinela.simulator.api;

import gt.umg.centinela.simulator.config.SimulatorProperties;
import gt.umg.centinela.simulator.generator.BaselineGenerator;
import gt.umg.centinela.simulator.publish.SpoolService;
import gt.umg.centinela.simulator.replay.CsvReplayService;
import gt.umg.centinela.simulator.scenario.ScenarioCatalog;
import gt.umg.centinela.simulator.scenario.ScenarioDefinition;
import gt.umg.centinela.simulator.scenario.ScenarioExecution;
import gt.umg.centinela.simulator.scenario.ScenarioRunner;
import gt.umg.centinela.simulator.validation.ValidationService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;

/**
 * API de control del simulador. Permite manejar la demo en vivo:
 * <pre>
 *   GET  /api/simulator/status
 *   POST /api/simulator/baseline?enabled=false
 *   POST /api/simulator/burst?multiplier=20&amp;seconds=60
 *   GET  /api/scenarios
 *   POST /api/scenarios/robo-en-curso?zone=4
 *   POST /api/replay?resource=noche-de-viernes.csv&amp;speed=2
 *   POST /api/validation?resetState=true   (y GET /api/validation/latest)
 * </pre>
 */
@RestController
@RequestMapping("/api")
public class SimulatorController {

    private final SimulatorProperties properties;
    private final BaselineGenerator baseline;
    private final SpoolService spool;
    private final ScenarioCatalog catalog;
    private final ScenarioRunner runner;
    private final CsvReplayService replay;
    private final ValidationService validation;

    public SimulatorController(SimulatorProperties properties, BaselineGenerator baseline, SpoolService spool,
                               ScenarioCatalog catalog, ScenarioRunner runner, CsvReplayService replay,
                               ValidationService validation) {
        this.properties = properties;
        this.baseline = baseline;
        this.spool = spool;
        this.catalog = catalog;
        this.runner = runner;
        this.replay = replay;
        this.validation = validation;
    }

    @GetMapping("/simulator/status")
    public Map<String, Object> status() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("node", properties.getNodeName());
        status.put("zones", properties.getZones());
        status.put("baselineEnabled", baseline.isEnabled());
        status.put("bursting", baseline.isBursting());
        status.put("multiplier", baseline.currentMultiplier());
        status.put("expectedEventsPerSecond", Math.round(baseline.expectedEventsPerSecond() * 100) / 100.0);
        status.put("ratesPerZone", properties.getRatesPerZone());
        status.put("spoolPending", spool.pendingCount());
        return status;
    }

    @PostMapping("/simulator/baseline")
    public Map<String, Object> baseline(@RequestParam boolean enabled) {
        baseline.setEnabled(enabled);
        return status();
    }

    @PostMapping("/simulator/burst")
    public Map<String, Object> burst(@RequestParam(defaultValue = "20") double multiplier,
                                     @RequestParam(defaultValue = "60") int seconds) {
        baseline.startBurst(multiplier, seconds);
        return status();
    }

    @GetMapping("/scenarios")
    public Collection<ScenarioDefinition> scenarios() {
        return catalog.all();
    }

    @PostMapping("/scenarios/{id}")
    public ScenarioExecution runScenario(@PathVariable String id,
                                         @RequestParam(required = false) Integer zone,
                                         @RequestParam(defaultValue = "1.0") double speed) {
        return runner.run(id, zone, speed);
    }

    @PostMapping("/replay")
    public CsvReplayService.ReplaySummary replay(@RequestParam(defaultValue = "noche-de-viernes.csv") String resource,
                                                 @RequestParam(defaultValue = "1.0") double speed,
                                                 @RequestParam(defaultValue = "true") boolean jitter) throws IOException {
        return replay.replay(resource, speed, jitter);
    }

    @PostMapping("/validation")
    public ValidationService.ValidationReport startValidation(@RequestParam(defaultValue = "true") boolean resetState) {
        return validation.start(resetState);
    }

    @GetMapping("/validation/latest")
    public ResponseEntity<ValidationService.ValidationReport> latestValidation() {
        return ResponseEntity.of(validation.latest());
    }

    @GetMapping("/validation/{id}")
    public ResponseEntity<ValidationService.ValidationReport> validation(@PathVariable UUID id) {
        return ResponseEntity.of(validation.find(id));
    }

    @ExceptionHandler({NoSuchElementException.class, IOException.class, IllegalArgumentException.class})
    public ResponseEntity<Map<String, String>> badRequest(Exception e) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", e.getMessage()));
    }
}
