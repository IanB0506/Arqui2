package gt.umg.centinela.common.kafka;

/**
 * Headers propios que acompanan a cada mensaje en la DLQ ({@code events.dlq}), ademas de los
 * estandar de Spring Kafka ({@code kafka_dlt-original-topic}, {@code kafka_dlt-exception-message}...).
 */
public final class DlqHeaders {

    /** PERMANENTE, TRANSITORIO o DESCONOCIDO (ver {@link FailureKind}). */
    public static final String ERROR_CLASS = "centinela-error-class";
    /** Servicio que no pudo procesar el mensaje (spring.application.name). */
    public static final String SERVICE = "centinela-service";
    /** Cuantas veces se reproceso desde la DLQ; el reproceso automatico se detiene en un tope. */
    public static final String ATTEMPTS = "centinela-dlq-attempts";

    private DlqHeaders() {
    }
}
