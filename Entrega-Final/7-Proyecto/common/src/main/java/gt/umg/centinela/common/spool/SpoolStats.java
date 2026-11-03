package gt.umg.centinela.common.spool;

import java.time.Instant;

public record SpoolStats(String name, int pending, int inFlight, long bytes, long fileBytes, Long oldestAgeSeconds,
                         long stored, long acked, long resent, long expired, long evicted, long refused,
                         long rejected, long invalidLines, long writeErrors, long syncs, String lastError,
                         Instant lastResendAt) {
}
