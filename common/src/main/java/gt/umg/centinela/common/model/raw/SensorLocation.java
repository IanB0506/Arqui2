package gt.umg.centinela.common.model.raw;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;

/** Ubicacion fija de un sensor (camaras LPR). */
public record SensorLocation(
        @JsonProperty("sensor_id") @NotBlank String sensorId,
        @JsonProperty("zona") String zoneName,
        @JsonProperty("latitud") Double latitude,
        @JsonProperty("longitud") Double longitude) {
}
