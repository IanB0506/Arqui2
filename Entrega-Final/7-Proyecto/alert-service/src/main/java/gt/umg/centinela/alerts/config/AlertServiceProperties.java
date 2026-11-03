package gt.umg.centinela.alerts.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.List;

@ConfigurationProperties(prefix = "alerts")
public record AlertServiceProperties(
        String plateHmacSecret,
        List<String> watchlistPlates,
        Duration plateRetention,
        Duration alertExpiry,
        Slo slo,
        Security security) {
    public record Slo(long latencyThresholdMs, double targetRatio, Duration window, long freshnessThresholdMs,
                      long overloadLag) {
    }

    public record Security(String operatorUser, String operatorPassword, String adminUser, String adminPassword) {
    }
}
