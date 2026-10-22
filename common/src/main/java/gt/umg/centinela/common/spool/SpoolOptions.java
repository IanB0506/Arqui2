package gt.umg.centinela.common.spool;

import java.nio.file.Path;
import java.time.Duration;

/**
 * Configuracion de un {@link DiskSpool}.
 *
 * @param name        nombre para logs y metricas
 * @param file        archivo del diario
 * @param fsync       true: cada append espera a que el dato este en disco (fsync agrupado)
 * @param maxBytes    tope de lo pendiente (en disco y en memoria)
 * @param maxAge      vencimiento de lo pendiente; null = no vence
 * @param overflow    que hacer si no cabe: rechazar lo nuevo o descartar lo mas viejo
 * @param maxAttempts fallas no transitorias antes de apartar el mensaje a {@code <archivo>.rejected}
 */
public record SpoolOptions(String name, Path file, boolean fsync, long maxBytes, Duration maxAge,
                           Overflow overflow, int maxAttempts) {

    public enum Overflow { REJECT_NEW, DROP_OLDEST }

    /** Eventos criticos: fsync, sin vencimiento y perdida cero (si no cabe, el llamador se entera). */
    public static SpoolOptions critical(String name, Path file, long maxBytes, int maxAttempts) {
        return new SpoolOptions(name, file, true, maxBytes, null, Overflow.REJECT_NEW, maxAttempts);
    }

    /** Telemetria de mejor esfuerzo: vence y, si se llena, descarta lo mas viejo. */
    public static SpoolOptions bestEffort(String name, Path file, long maxBytes, Duration maxAge, boolean fsync,
                                          int maxAttempts) {
        return new SpoolOptions(name, file, fsync, maxBytes, maxAge, Overflow.DROP_OLDEST, maxAttempts);
    }
}
