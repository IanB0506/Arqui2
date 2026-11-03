package gt.umg.centinela.common.model;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record AlertEvent(
        UUID alertId,
        int revision,
        String ruleId,
        String alertType,
        Severity severity,
        AlertStatus status,
        int zoneId,
        String zoneName,
        double latitude,
        double longitude,
        double confidence,
        String title,
        String description,
        Instant createdAt,
        Instant firstEventTime,
        Instant triggerEventTime,
        List<Evidence> evidence,
        String plateMasked,
        String plateHash,
        UUID parentAlertId) {
}
