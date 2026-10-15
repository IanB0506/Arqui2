package gt.umg.centinela.common.spool;

import java.time.Instant;

/** Foto inmutable del estado de un spool, para la API, los logs y las metricas. */
public record SpoolStats(String name, int pending, int inFlight, long bytes, long fileBytes, Long oldestAgeSeconds,
                         long stored, long acked, long resent, long expired, long evicted, long refused,
                         long rejected, long invalidLines, long writeErrors, long syncs, String lastError,
                         Instant lastResendAt) {
}
