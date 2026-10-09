package gt.umg.centinela.common.model.raw;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.time.Instant;
import java.util.UUID;

/**
 * Reporte ciudadano voluntario. Campos del enunciado: tipo_evento, mensaje_descriptivo,
 * ubicacion_aproximada, origen y timestamp. Se agrega el id del reportante para poder contar
 * reportes de personas distintas (se seudonimiza en la ingesta).
 */
public record CitizenReportEvent(
        @JsonProperty("event_id") @NotNull UUID eventId,
        @JsonProperty("timestamp") @NotNull Instant timestamp,
        @JsonProperty("tipo_evento") @NotNull CitizenEventType eventType,
        @JsonProperty("mensaje_descriptivo") @Size(max = 500) String message,
        @JsonProperty("ubicacion_aproximada") @NotNull @Valid ZoneRef location,
        @JsonProperty("origen") @NotNull ReportOrigin origin,
        @JsonProperty("reportante_id") String reporterId) {
}
