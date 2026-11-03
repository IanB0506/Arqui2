package gt.umg.centinela.simulator.scenario;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

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
