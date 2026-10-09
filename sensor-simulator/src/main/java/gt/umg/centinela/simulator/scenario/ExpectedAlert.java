package gt.umg.centinela.simulator.scenario;

/**
 * Resultado esperado de un escenario (seccion 2.4 del enunciado): que alerta deberia generar el
 * motor de correlacion, en que zona y en cuanto tiempo como maximo.
 */
public record ExpectedAlert(String alertType, int zoneId, int withinSeconds, String minSeverity) {
}
