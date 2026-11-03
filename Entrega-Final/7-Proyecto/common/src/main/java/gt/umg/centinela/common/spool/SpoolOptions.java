package gt.umg.centinela.common.spool;

import java.nio.file.Path;
import java.time.Duration;

public record SpoolOptions(String name, Path file, boolean fsync, long maxBytes, Duration maxAge,
                           Overflow overflow, int maxAttempts) {
    public enum Overflow { REJECT_NEW, DROP_OLDEST }

    public static SpoolOptions critical(String name, Path file, long maxBytes, int maxAttempts) {
        return new SpoolOptions(name, file, true, maxBytes, null, Overflow.REJECT_NEW, maxAttempts);
    }

    public static SpoolOptions bestEffort(String name, Path file, long maxBytes, Duration maxAge, boolean fsync,
                                          int maxAttempts) {
        return new SpoolOptions(name, file, fsync, maxBytes, maxAge, Overflow.DROP_OLDEST, maxAttempts);
    }
}
