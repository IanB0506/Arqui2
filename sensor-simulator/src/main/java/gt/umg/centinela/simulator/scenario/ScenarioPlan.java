package gt.umg.centinela.simulator.scenario;

import java.util.List;

/** Pasos concretos de un escenario para una zona, mas las alertas que se esperan. */
public record ScenarioPlan(List<ScenarioStep> steps, List<ExpectedAlert> expected) {
}
