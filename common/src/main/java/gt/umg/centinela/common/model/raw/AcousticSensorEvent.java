package gt.umg.centinela.common.model.raw;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;
import java.util.UUID;

/**
 * Deteccion de un sensor acustico. Campos del enunciado: tipo_sonido_detectado, nivel_decibeles,
 * zona, timestamp y probabilidad_de_evento_critico.
 */
public record AcousticSensorEvent(
        @JsonProperty("event_id") @NotNull UUID eventId,
        @JsonProperty("timestamp") @NotNull Instant timestamp,
        @JsonProperty("sensor_id") @NotBlank String sensorId,
        @JsonProperty("zona") @NotBlank String zoneName,
        @JsonProperty("tipo_sonido_detectado") @NotNull SoundType soundType,
        @JsonProperty("nivel_decibeles") @NotNull @DecimalMin("0") @DecimalMax("200") Double decibels,
        @JsonProperty("probabilidad_evento_critico") @NotNull @DecimalMin("0") @DecimalMax("1") Double criticalProbability) {
}
