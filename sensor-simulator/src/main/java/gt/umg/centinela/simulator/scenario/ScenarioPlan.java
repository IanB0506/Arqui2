package gt.umg.centinela.simulator.scenario;

import java.util.List;

/**
 * Pasos concretos de un escenario para una zona, mas las alertas que se esperan.
 *
 * @param zones zonas donde el escenario emite eventos: se aislan del trafico de fondo mientras dura
 */
public record ScenarioPlan(List<ScenarioStep> steps, List<ExpectedAlert> expected, List<Integer> zones) {

    /** Plan que solo emite eventos en las zonas de sus alertas esperadas. */
    public ScenarioPlan(List<ScenarioStep> steps, List<ExpectedAlert> expected) {
        this(steps, expected, expected.stream().map(ExpectedAlert::zoneId).distinct().toList());
    }

    /** Zonas de las alertas esperadas: alli el escenario arranca sin el historial reciente del fondo. */
    public List<Integer> expectedZones() {
        return expected.stream().map(ExpectedAlert::zoneId).distinct().sorted().toList();
    }
}
