package gt.umg.centinela.common.model.raw;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.time.Instant;
import java.util.UUID;

public record SpeedSensorEvent(
        @JsonProperty("event_id") @NotNull UUID eventId,
        @JsonProperty("timestamp") @NotNull Instant timestamp,
        @JsonProperty("sensor_id") @NotBlank String sensorId,
        @JsonProperty("zona") @NotBlank String zoneName,
        @JsonProperty("velocidad_detectada") @NotNull @DecimalMin("0") @DecimalMax("300") Double speed,
        @JsonProperty("direccion") @NotNull Direction direction,
        @JsonProperty("contravia") Boolean wrongWay,
        @JsonProperty("limite_velocidad") @Positive Integer speedLimit) {
}
