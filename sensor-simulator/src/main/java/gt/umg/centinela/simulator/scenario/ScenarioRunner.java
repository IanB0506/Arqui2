package gt.umg.centinela.simulator.scenario;

import gt.umg.centinela.simulator.admin.AlertServiceAdminClient;
import gt.umg.centinela.simulator.generator.QuietZones;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Ejecuta escenarios programando cada paso en el tiempo indicado.
 *
 * <p>Cada escenario corre aislado del trafico de fondo para que su resultado sea reproducible: en sus
 * zonas el fondo solo emite lecturas normales hasta que se cierra la ventana de sus reglas, y en las zonas
 * de sus alertas esperadas arranca sin el historial reciente ni incidentes abiertos (un exceso de velocidad
 * aleatorio de hace un minuto no debe corroborar el panico de la fila 2 de la tabla 2.4, y el robo de la
 * fila 1 debe salir como alerta nueva aunque haya otro incidente abierto en la Zona 4).
 */
@Component
public class ScenarioRunner {

    private static final Logger log = LoggerFactory.getLogger(ScenarioRunner.class);
    /** Ventana posterior mas larga de las reglas (R1: 5 min) mas un margen: hasta entonces la evidencia cuenta. */
    static final Duration QUIET_AFTER = Duration.ofMinutes(5).plusSeconds(30);
    private static final DateTimeFormatter HOUR = DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault());

    private final ScenarioCatalog catalog;
    private final TaskScheduler scheduler;
    private final QuietZones quietZones;
    private final AlertServiceAdminClient correlation;

    public ScenarioRunner(ScenarioCatalog catalog, TaskScheduler scheduler, QuietZones quietZones,
                          AlertServiceAdminClient correlation) {
        this.catalog = catalog;
        this.scheduler = scheduler;
        this.quietZones = quietZones;
        this.correlation = correlation;
    }

    /**
     * @param id    escenario
     * @param zone  zona donde ocurre (null = zona por defecto del escenario)
     * @param speed factor de aceleracion del tiempo (2.0 = la mitad de espera entre pasos)
     */
    public ScenarioExecution run(String id, Integer zone, double speed) {
        ScenarioDefinition definition = catalog.find(id)
                .orElseThrow(() -> new NoSuchElementException("Escenario inexistente: " + id));
        int zoneId = zone == null || definition.fixedZone() ? definition.defaultZone() : zone;
        double factor = speed <= 0 ? 1.0 : speed;
        ScenarioPlan plan = definition.planner().apply(zoneId);
        Instant start = Instant.now();
        long lastOffset = plan.steps().stream().mapToLong(ScenarioStep::offsetMillis).max().orElse(0);
        Instant quietUntil = start.plusMillis((long) (lastOffset / factor)).plus(QUIET_AFTER);
        String isolation = isolate(plan, quietUntil); // antes del primer paso
        List<ScenarioExecution.PlannedStep> planned = new ArrayList<>();
        for (ScenarioStep step : plan.steps()) {
            Instant at = start.plusMillis((long) (step.offsetMillis() / factor));
            planned.add(new ScenarioExecution.PlannedStep(at, step.description()));
            scheduler.schedule(() -> {
                log.info("[escenario {}] {}", id, step.description());
                step.action().run();
            }, at);
        }
        log.info("Escenario '{}' lanzado en Zona {} ({} pasos). {}", id, zoneId, plan.steps().size(), isolation);
        return new ScenarioExecution(UUID.randomUUID(), id, definition.title(), zoneId, start, planned, plan.expected(),
                plan.zones(), quietUntil, Duration.between(start, quietUntil).toSeconds(), isolation);
    }

    private String isolate(ScenarioPlan plan, Instant quietUntil) {
        quietZones.reserve(plan.zones(), quietUntil);
        String summary = "Zonas " + list(plan.zones()) + " sin ruido de fondo hasta las " + HOUR.format(quietUntil);
        try {
            correlation.resetZones(plan.expectedZones());
            return summary + "; historial reciente e incidentes abiertos de " + list(plan.expectedZones()) + " reiniciados.";
        } catch (RuntimeException e) {
            log.warn("No se pudo reiniciar el historial de las zonas {}: {}", plan.expectedZones(), e.getMessage());
            return summary + "; no se pudo reiniciar el historial reciente (alert-service sin respuesta).";
        }
    }

    private static String list(List<Integer> zones) {
        return zones.stream().map(String::valueOf).collect(Collectors.joining(", "));
    }
}
