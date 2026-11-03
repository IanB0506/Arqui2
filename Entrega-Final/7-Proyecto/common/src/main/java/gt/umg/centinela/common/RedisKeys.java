package gt.umg.centinela.common;

public final class RedisKeys {
    public static final String WATCHLIST = "watchlist:plates";
    public static final String WATCHLIST_SYNCED = "watchlist:synced";
    public static final String THRESHOLD_ADJUSTMENTS = "correlation:threshold-adjustments";
    public static final String DEGRADED = "system:degraded";
    public static final String CORRELATION_BACKLOG = "correlation:backlog";

    private RedisKeys() {
    }
}
