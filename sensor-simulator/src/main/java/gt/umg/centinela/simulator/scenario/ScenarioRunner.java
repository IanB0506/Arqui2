package gt.umg.centinela.simulator.scenario;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;

/** Ejecuta escenarios programando cada paso en el tiempo indicado. */
@Component
public class ScenarioRunner {

    private static final Logger log = LoggerFactory.getLogger(ScenarioRunner.class);

    private final ScenarioCatalog catalog;
    private final TaskScheduler scheduler;

    public ScenarioRunner(ScenarioCatalog catalog, TaskScheduler scheduler) {
        this.catalog = catalog;
        this.scheduler = scheduler;
    }

    /**
     * @param id    escenario
     * @param zone  zona donde ocurre (null = zona por defecto del escenario)
     * @param speed factor de aceleracion del tiempo (2.0 = la mitad de espera entre pasos)
     */
    public ScenarioExecution run(String id, Integer zone, double speed) {
        ScenarioDefinition definition = catalog.find(id)
                .orElseThrow(() -> new NoSuchElementException("Escenario inexistente: " + id));
        int zoneId = zone == null ? definition.defaultZone() : zone;
        double factor = speed <= 0 ? 1.0 : speed;
        ScenarioPlan plan = definition.planner().apply(zoneId);
        Instant start = Instant.now();
        List<ScenarioExecution.PlannedStep> planned = new ArrayList<>();
        for (ScenarioStep step : plan.steps()) {
            Instant at = start.plusMillis((long) (step.offsetMillis() / factor));
            planned.add(new ScenarioExecution.PlannedStep(at, step.description()));
            scheduler.schedule(() -> {
                log.info("[escenario {}] {}", id, step.description());
                step.action().run();
            }, at);
        }
        log.info("Escenario '{}' lanzado en Zona {} ({} pasos)", id, zoneId, plan.steps().size());
        return new ScenarioExecution(UUID.randomUUID(), id, definition.title(), zoneId, start, planned, plan.expected());
    }
}
