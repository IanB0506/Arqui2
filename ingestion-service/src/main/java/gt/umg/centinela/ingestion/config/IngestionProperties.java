package gt.umg.centinela.ingestion.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Configuracion de la ingesta (prefijo {@code ingestion}).
 *
 * @param plateHmacSecret       llave secreta para seudonimizar placas e identificadores
 * @param speedTolerance        tolerancia sobre el limite: 0.3 = se considera exceso a partir de 130 %
 * @param defaultSpeedLimit     limite de velocidad cuando el sensor no lo reporta (km/h)
 * @param coordinateDecimals    decimales que se conservan en ubicaciones de ciudadanos (3 = ~110 m)
 * @param maxClockSkew          cuanto puede venir adelantado el reloj de un sensor
 * @param maxEventAge           eventos mas antiguos se rechazan (salvo que se permitan historicos)
 * @param allowHistorical       acepta eventos antiguos (util para replays historicos)
 * @param dedupTtl              tiempo durante el que se recuerda un event_id para descartar duplicados
 * @param degradedSampleRate    fraccion de lecturas de velocidad normales que se conservan en modo degradado
 */
@ConfigurationProperties(prefix = "ingestion")
public record IngestionProperties(
        String plateHmacSecret,
        double speedTolerance,
        int defaultSpeedLimit,
        int coordinateDecimals,
        Duration maxClockSkew,
        Duration maxEventAge,
        boolean allowHistorical,
        Duration dedupTtl,
        double degradedSampleRate) {
}
