package gt.umg.centinela.simulator.scenario;

/**
 * Paso de un escenario: una accion (normalmente publicar uno o mas eventos) que se ejecuta a cierto
 * tiempo desde el inicio del escenario.
 */
public record ScenarioStep(long offsetMillis, String description, Runnable action) {
}
