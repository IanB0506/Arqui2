package gt.umg.centinela.simulator.scenario;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Respuesta al lanzar un escenario: que se va a emitir, cuando, y que alertas se esperan. */
public record ScenarioExecution(
        UUID executionId,
        String scenarioId,
        String title,
        int zoneId,
        Instant startedAt,
        List<PlannedStep> steps,
        List<ExpectedAlert> expected) {

    public record PlannedStep(Instant at, String description) {
    }
}
