package gt.umg.centinela.common.model;

public enum Severity {
    LOW, MEDIUM, HIGH, CRITICAL;

    public boolean isHigherThan(Severity other) {
        return other == null || this.ordinal() > other.ordinal();
    }
}
