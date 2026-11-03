package gt.umg.centinela.ingestion.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "ingestion.gateway")
public record GatewayProperties(
        String spoolDir,
        Duration ackTimeout,
        Duration kafkaDownBackoff,
        int maxAttempts,
        int maxSizeMb,
        boolean fsync) {
}
