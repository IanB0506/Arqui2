package gt.umg.centinela.common;

public final class Topics {
    public static final String RAW_PANIC = "sensor.panic.raw";
    public static final String RAW_LPR = "sensor.lpr.raw";
    public static final String RAW_SPEED = "sensor.speed.raw";
    public static final String RAW_ACOUSTIC = "sensor.acoustic.raw";
    public static final String RAW_CITIZEN = "sensor.citizen.raw";

    public static final String NORMALIZED = "events.normalized";
    public static final String ALERTS = "alerts";
    public static final String DLQ = "events.dlq";

    public static final int PARTITIONS = 12;
    public static final int ALERT_PARTITIONS = 3;

    private Topics() {
    }
}
