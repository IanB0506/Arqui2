package gt.umg.centinela.common.model.raw;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;
import java.util.UUID;

/**
 * Boton de panico ciudadano. Campos del enunciado: timestamp, zona_geografica, tipo_de_alerta,
 * identificador_del_dispositivo_simulado y user_context (si proviene de la app movil).
 */
public record PanicButtonEvent(
        @JsonProperty("event_id") @NotNull UUID eventId,
        @JsonProperty("timestamp") @NotNull Instant timestamp,
        @JsonProperty("zona_geografica") @NotNull @Valid ZoneRef zone,
        @JsonProperty("tipo_alerta") @NotNull PanicAlertType alertType,
        @JsonProperty("identificador_dispositivo") @NotBlank String deviceId,
        @JsonProperty("user_context") UserContext userContext) {
}
