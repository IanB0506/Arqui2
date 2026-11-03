package gt.umg.centinela.ingestion.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

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
