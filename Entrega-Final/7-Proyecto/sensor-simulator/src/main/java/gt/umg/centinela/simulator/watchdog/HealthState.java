package gt.umg.centinela.simulator.watchdog;

public enum HealthState {
    UNKNOWN(-1), UP(1), DEGRADED(0.5), DOWN(0);

    private final double value;

    HealthState(double value) {
        this.value = value;
    }

    public double value() {
        return value;
    }
}
