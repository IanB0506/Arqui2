package gt.umg.centinela.common.model;

/** Severidad de una alerta, de menor a mayor. */
public enum Severity {
    LOW, MEDIUM, HIGH, CRITICAL;

    public boolean isHigherThan(Severity other) {
        return other == null || this.ordinal() > other.ordinal();
    }
}
