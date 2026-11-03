package gt.umg.centinela.common.kafka;

public final class DlqHeaders {
    public static final String ERROR_CLASS = "centinela-error-class";
    public static final String SERVICE = "centinela-service";
    public static final String ATTEMPTS = "centinela-dlq-attempts";

    private DlqHeaders() {
    }
}
