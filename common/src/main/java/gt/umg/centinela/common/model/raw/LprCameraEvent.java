package gt.umg.centinela.common.model.raw;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

import java.time.Instant;
import java.util.UUID;

/**
 * Lectura de una camara LPR. Campos del enunciado: placa_vehicular, ubicacion_sensor,
 * velocidad_estimada, timestamp y modelo/color del vehiculo.
 */
public record LprCameraEvent(
        @JsonProperty("event_id") @NotNull UUID eventId,
        @JsonProperty("timestamp") @NotNull Instant timestamp,
        @JsonProperty("placa_vehicular") @NotBlank @Pattern(regexp = "^[A-Za-z0-9 -]{4,12}$") String plate,
        @JsonProperty("ubicacion_sensor") @NotNull @Valid SensorLocation location,
        @JsonProperty("velocidad_estimada") @NotNull @DecimalMin("0") @DecimalMax("300") Double estimatedSpeed,
        @JsonProperty("modelo_vehiculo") String vehicleModel,
        @JsonProperty("color_vehiculo") String vehicleColor) {
}
