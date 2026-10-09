package gt.umg.centinela.ingestion.pipeline;

import gt.umg.centinela.common.json.InvalidEventException;
import gt.umg.centinela.common.model.NormalizedEvent;
import gt.umg.centinela.common.model.SensorType;
import gt.umg.centinela.common.privacy.PlatePseudonymizer;
import gt.umg.centinela.ingestion.config.IngestionProperties;
import jakarta.validation.Validation;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EventNormalizerTest {

    private static final Instant NOW = Instant.parse("2026-10-30T04:05:00Z");
    private final IngestionProperties properties = new IngestionProperties("secreto-de-pruebas-123", 0.3, 60, 3,
            Duration.ofMinutes(2), Duration.ofHours(24), false, Duration.ofMinutes(10), 0.1);
    private final PlatePseudonymizer pseudonymizer = new PlatePseudonymizer(properties.plateHmacSecret());
    private final EventNormalizer normalizer = new EventNormalizer(properties, pseudonymizer,
            Validation.buildDefaultValidatorFactory().getValidator(), Clock.fixed(NOW, ZoneOffset.UTC));

    @Test
    void lprPlateIsPseudonymizedAndSpeedingDetected() {
        String json = """
                {"event_id":"7c9e6679-7425-40de-944b-e07fc1f90ae7","timestamp":"2026-10-30T04:04:58Z",
                 "placa_vehicular":"XYZ123","velocidad_estimada":85.0,
                 "ubicacion_sensor":{"sensor_id":"LPR-Z04-01","zona":"Zona 4","latitud":14.64,"longitud":-90.50}}
                """;
        NormalizedEvent event = normalizer.normalize(SensorType.LPR, json);
        assertThat(event.zoneId()).isEqualTo(4);
        assertThat(event.anomalous()).isTrue();
        assertThat(event.subtype()).isEqualTo("EXCESO_VELOCIDAD");
        assertThat(event.plateMasked()).isEqualTo("XYZ***");
        assertThat(event.plateHash()).isEqualTo(pseudonymizer.pseudonymizePlate("XYZ123"));
        assertThat(event.toString()).doesNotContain("XYZ123");
    }

    @Test
    void citizenReportResolvesZoneFromCoordinatesAndGeneralizesLocation() {
        String json = """
                {"event_id":"1b9d6bcd-bbfd-4b2d-9b5d-ab8dfbbd4bed","timestamp":"2026-10-30T04:04:00Z",
                 "tipo_evento":"ACCIDENTE","origen":"APP","reportante_id":"CIU-00042",
                 "ubicacion_aproximada":{"latitud":14.621234,"longitud":-90.538765}}
                """;
        NormalizedEvent event = normalizer.normalize(SensorType.CITIZEN_REPORT, json);
        assertThat(event.zoneId()).isEqualTo(6);
        assertThat(event.latitude()).isEqualTo(14.621);
        assertThat(event.longitude()).isEqualTo(-90.539);
        assertThat(event.sourceHash()).isNotNull().doesNotContain("CIU");
    }

    @Test
    void speedBelowToleranceIsNormal() {
        String json = """
                {"event_id":"2b9d6bcd-bbfd-4b2d-9b5d-ab8dfbbd4bed","timestamp":"2026-10-30T04:04:00Z",
                 "sensor_id":"VEL-Z04-02","zona":"Zona 4","velocidad_detectada":75.0,"direccion":"NORTE",
                 "limite_velocidad":60}
                """;
        NormalizedEvent event = normalizer.normalize(SensorType.SPEED, json);
        assertThat(event.anomalous()).isFalse();
        assertThat(event.subtype()).isEqualTo("NORMAL");
    }

    @Test
    void invalidEventsAreRejectedAsPermanentErrors() {
        assertThatThrownBy(() -> normalizer.normalize(SensorType.PANIC, "{esto no es json"))
                .isInstanceOf(InvalidEventException.class);
        assertThatThrownBy(() -> normalizer.normalize(SensorType.SPEED, """
                {"event_id":"2b9d6bcd-bbfd-4b2d-9b5d-ab8dfbbd4bed","timestamp":"2026-10-30T04:04:00Z",
                 "sensor_id":"VEL-Z99-02","zona":"Zona 99","velocidad_detectada":75.0,"direccion":"NORTE"}
                """)).isInstanceOf(InvalidEventException.class).hasMessageContaining("Zona desconocida");
        assertThatThrownBy(() -> normalizer.normalize(SensorType.ACOUSTIC, """
                {"event_id":"3b9d6bcd-bbfd-4b2d-9b5d-ab8dfbbd4bed","timestamp":"2026-10-30T05:00:00Z",
                 "sensor_id":"ACU-Z02-01","zona":"Zona 2","tipo_sonido_detectado":"DISPARO",
                 "nivel_decibeles":120,"probabilidad_evento_critico":0.9}
                """)).isInstanceOf(InvalidEventException.class).hasMessageContaining("futuro");
    }

    @Test
    void appPanicDeviceIsPseudonymized() {
        String json = """
                {"event_id":"4b9d6bcd-bbfd-4b2d-9b5d-ab8dfbbd4bed","timestamp":"2026-10-30T04:04:00Z",
                 "zona_geografica":{"nombre":"Zona 10","latitud":14.6012345,"longitud":-90.5398765},
                 "tipo_alerta":"ROBO","identificador_dispositivo":"APP-12345",
                 "user_context":{"usuario_id":"CIU-12345","plataforma":"ANDROID","version_app":"2.3.1"}}
                """;
        NormalizedEvent event = normalizer.normalize(SensorType.PANIC, json);
        assertThat(event.zoneId()).isEqualTo(10);
        assertThat(event.sensorId()).startsWith("APP-").doesNotContain("12345");
        assertThat(event.latitude()).isEqualTo(14.601);
        assertThat(event.attributes()).doesNotContainKey("usuario_id");
    }
}
