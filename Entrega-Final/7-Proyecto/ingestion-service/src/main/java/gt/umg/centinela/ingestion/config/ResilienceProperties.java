package gt.umg.centinela.ingestion.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

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
