package gt.umg.centinela.simulator.scenario;

public record ScenarioStep(long offsetMillis, String description, Runnable action) {
}
