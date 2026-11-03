package gt.umg.centinela.simulator.scenario;

public record ExpectedAlert(String alertType, int zoneId, int withinSeconds, String minSeverity) {
}
