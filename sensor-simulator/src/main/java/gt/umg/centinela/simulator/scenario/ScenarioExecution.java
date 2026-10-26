package gt.umg.centinela.simulator.scenario;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Respuesta al lanzar un escenario: que se va a emitir, cuando, y que alertas se esperan.
 *
 * @param quietZones zonas aisladas del trafico de fondo (solo lecturas normales) hasta {@code quietUntil}
 * @param quietSeconds cuanto dura ese aislamiento desde el lanzamiento (para avisar a otros simuladores)
 * @param isolation  resumen del aislamiento, para mostrarlo al operador
 */
public record ScenarioExecution(
        UUID executionId,
        String scenarioId,
        String title,
        int zoneId,
        Instant startedAt,
        List<PlannedStep> steps,
        List<ExpectedAlert> expected,
        List<Integer> quietZones,
        Instant quietUntil,
        long quietSeconds,
        String isolation) {

    public record PlannedStep(Instant at, String description) {
    }
}
