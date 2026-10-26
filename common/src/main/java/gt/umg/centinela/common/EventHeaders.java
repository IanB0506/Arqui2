package gt.umg.centinela.common;

/**
 * Headers de Kafka que agregan los productores del borde (simulador y gateway de la app) a los
 * mensajes crudos. Son solo nombres: common no depende de Kafka para definirlos.
 */
public final class EventHeaders {

    /**
     * Hora del primer intento de envio desde el borde, en milisegundos epoch (texto decimal). Se
     * conserva aunque el evento se reenvie desde un spool minutos despues: la ingesta mide la edad
     * maxima del evento contra esta hora y no contra la hora de proceso.
     */
    public static final String FIRST_ATTEMPT = "centinela-first-attempt";

    /** "true" si el mensaje se reenvio desde un spool en disco (llego tarde por una caida). */
    public static final String SPOOLED = "centinela-spooled";

    private EventHeaders() {
    }
}
