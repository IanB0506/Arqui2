package gt.umg.centinela.common;

/** Llaves de Redis que comparten varios servicios. */
public final class RedisKeys {

    /** Set de seudonimos de placas robadas (lo carga alert-service desde la base, lo lee el motor). */
    public static final String WATCHLIST = "watchlist:plates";
    /**
     * Centinela de la lista de placas: alert-service la escribe al cargar la lista. Si falta, Redis
     * se reinicio vacio (Redis no persiste): alert-service resincroniza de inmediato y el motor
     * conserva su copia local en vez de reemplazarla por una lista vacia.
     */
    public static final String WATCHLIST_SYNCED = "watchlist:synced";
    /** Hash con los ajustes adaptativos de umbral por regla y zona. */
    public static final String THRESHOLD_ADJUSTMENTS = "correlation:threshold-adjustments";
    /** "1" mientras el SLO se incumple por sobrecarga: la ingesta muestrea lecturas normales. */
    public static final String DEGRADED = "system:degraded";
    /** Alguna instancia del motor esta procesando atraso: no clasificar falsos positivos todavia. */
    public static final String CORRELATION_BACKLOG = "correlation:backlog";

    private RedisKeys() {
    }
}
