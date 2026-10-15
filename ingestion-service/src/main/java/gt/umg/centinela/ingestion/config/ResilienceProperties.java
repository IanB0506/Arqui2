package gt.umg.centinela.ingestion.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Resiliencia de la ingesta (prefijo {@code ingestion.resilience}, ADR-006).
 *
 * @param transientInitialInterval primera espera al reintentar un error transitorio o desconocido
 * @param transientMaxInterval     espera maxima entre reintentos ilimitados de errores transitorios
 *                                 (debe quedar muy por debajo de max.poll.interval.ms)
 * @param unknownMaxAttempts       reintentos de un error no clasificado antes de ir a la DLQ
 * @param retryLogEvery            un WARN en el primer reintento y luego uno cada N
 * @param publishTimeout           espera del ack al publicar el evento normalizado (mayor que
 *                                 delivery.timeout.ms, para no republicar algo que si iba a llegar)
 * @param redisRetryAfter          con Redis caido, cada cuanto vuelve a probar un solo hilo
 * @param dedupPendingTtl          vida de una marca de deduplicacion "pendiente" (si el proceso muere)
 * @param degradedStaleAfter       si la bandera de degradacion no se puede leer durante este tiempo,
 *                                 se deja de muestrear (es el TTL con que el alert-service la renueva)
 */
@ConfigurationProperties(prefix = "ingestion.resilience")
public record ResilienceProperties(
        Duration transientInitialInterval,
        Duration transientMaxInterval,
        int unknownMaxAttempts,
        int retryLogEvery,
        Duration publishTimeout,
        Duration redisRetryAfter,
        Duration dedupPendingTtl,
        Duration degradedStaleAfter) {
}
