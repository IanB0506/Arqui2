package gt.umg.centinela.common.model;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** La evidencia de una alerta lleva el dato no estructurado del evento, no solo su tipo. */
class EvidenceTest {

    @Test
    void elReporteCiudadanoLlevaSuTexto() {
        Evidence e = Evidence.of(event(SensorType.CITIZEN_REPORT, "APP", Map.of("mensaje", "Choque fuerte en el cruce")), 0.2);
        assertThat(e.detail()).isEqualTo("\"Choque fuerte en el cruce\"");
    }

    @Test
    void lasLecturasLlevanSuContexto() {
        assertThat(Evidence.of(event(SensorType.LPR, "SENSOR", Map.of("modelo", "Toyota Hilux", "color", "Blanco")), 0.2).detail())
                .isEqualTo("Toyota Hilux blanco");
        assertThat(Evidence.of(event(SensorType.SPEED, "SENSOR", Map.of("direccion", "NORTE", "limite", "60")), 0.3).detail())
                .isEqualTo("sentido norte, limite 60 km/h");
        assertThat(Evidence.of(event(SensorType.PANIC, "APP", Map.of("plataforma", "ANDROID")), 0.4).detail())
                .isEqualTo("desde la app (ANDROID)");
        assertThat(Evidence.of(event(SensorType.PANIC, "PUNTO_FISICO", Map.of()), 0.4).detail()).isEqualTo("boton fisico");
    }

    private static NormalizedEvent event(SensorType type, String origin, Map<String, String> attributes) {
        Instant t = Instant.parse("2026-10-31T22:05:00Z");
        return new NormalizedEvent(UUID.randomUUID(), type, "S-1", 4, "Zona 4", 14.6, -90.5, t, t, "X", null, 0.9,
                true, null, null, null, origin, attributes);
    }
}
