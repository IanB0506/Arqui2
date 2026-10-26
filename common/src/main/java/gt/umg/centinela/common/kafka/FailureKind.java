package gt.umg.centinela.common.kafka;

/** Clase de falla al procesar o enviar un mensaje (ADR-006). */
public enum FailureKind {
    /** El mensaje esta mal: reintentar no sirve. Va directo a la DLQ. */
    PERMANENTE,
    /** Una dependencia esta caida o lenta (Kafka, Redis, base de datos): se reintenta sin limite. */
    TRANSITORIO,
    /** No clasificada (por ejemplo un error de programacion): pocos reintentos y luego DLQ. */
    DESCONOCIDO
}
