package gt.umg.centinela.simulator.scenario;

import java.util.List;

public record ScenarioPlan(List<ScenarioStep> steps, List<ExpectedAlert> expected, List<Integer> zones) {
    public ScenarioPlan(List<ScenarioStep> steps, List<ExpectedAlert> expected) {
        this(steps, expected, expected.stream().map(ExpectedAlert::zoneId).distinct().toList());
    }

    public List<Integer> expectedZones() {
        return expected.stream().map(ExpectedAlert::zoneId).distinct().sorted().toList();
    }
}
