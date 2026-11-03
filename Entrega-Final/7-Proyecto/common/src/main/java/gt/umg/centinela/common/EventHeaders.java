package gt.umg.centinela.common;

public final class EventHeaders {
    // Hora del primer intento de envío: la ingesta mide la antigüedad desde aquí aunque el evento llegue tarde desde un spool.
    public static final String FIRST_ATTEMPT = "centinela-first-attempt";

    public static final String SPOOLED = "centinela-spooled";

    private EventHeaders() {
    }
}
