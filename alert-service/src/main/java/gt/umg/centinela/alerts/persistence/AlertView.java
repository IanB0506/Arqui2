package gt.umg.centinela.alerts.persistence;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.UUID;

/** Alerta tal como la ve el operador (sin seudonimos de placa). */
public record AlertView(
        UUID alertId,
        int revision,
        String ruleId,
        String alertType,
        String severity,
        String status,
        int zoneId,
        String zoneName,
        Double latitude,
        Double longitude,
        Double confidence,
        String title,
        String description,
        Instant createdAt,
        Instant updatedAt,
        Instant triggerEventTime,
        Long latencyMs,
        String plateMasked,
        JsonNode evidence,
        UUID parentAlertId,
        String validatedBy,
        Instant validatedAt,
        String resolutionNotes) {
}
