package gt.umg.centinela.simulator.watchdog;

/** Estado de un objetivo vigilado; el valor numerico es el que se exporta como metrica. */
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
