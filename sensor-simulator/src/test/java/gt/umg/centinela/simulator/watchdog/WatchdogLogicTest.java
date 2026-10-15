package gt.umg.centinela.simulator.watchdog;

import gt.umg.centinela.simulator.watchdog.WatchdogStatus.Observation;
import gt.umg.centinela.simulator.watchdog.WatchdogStatus.Transition;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class WatchdogLogicTest {

    private static final Instant T0 = Instant.parse("2026-10-15T12:00:00Z");

    @Test
    void caidoSoloTrasDosFallasSeguidasYLaVueltaEsInmediata() {
        HealthTracker t = new HealthTracker("alert-service", "http://b:8083", 2);
        assertThat(t.observe(new Observation(HealthState.UP, 5L, "UP"), T0)).isPresent();
        assertThat(t.observe(new Observation(HealthState.DOWN, null, "conexion rechazada"), T0.plusSeconds(10))).isEmpty();
        assertThat(t.state()).isEqualTo(HealthState.UP);
        Optional<Transition> down = t.observe(new Observation(HealthState.DOWN, null, "conexion rechazada"), T0.plusSeconds(20));
        assertThat(down).hasValueSatisfying(tr -> assertThat(tr.to()).isEqualTo(HealthState.DOWN));
        Optional<Transition> up = t.observe(new Observation(HealthState.UP, 4L, "UP"), T0.plusSeconds(150));
        assertThat(up).hasValueSatisfying(tr -> assertThat(tr.previousDurationSeconds()).isEqualTo(130));
    }

    @Test
    void unServicioQueRespondeConErrorEstaDegradadoYDiceQueDependenciaCayo() {
        Observation o = HttpProbe.classify(503, "{\"status\":\"DOWN\",\"components\":{\"redis\":{\"status\":\"DOWN\"},"
                + "\"ping\":{\"status\":\"UP\"}}}", 12);
        assertThat(o.state()).isEqualTo(HealthState.DEGRADED);
        assertThat(o.detail()).contains("redis DOWN").doesNotContain("ping");
        assertThat(HttpProbe.classify(200, "{\"status\":\"UP\"}", 3).state()).isEqualTo(HealthState.UP);
        assertThat(HttpProbe.classify(200, "Prometheus Server is Healthy.", 3).state()).isEqualTo(HealthState.UP);
        assertThat(HttpProbe.classify(401, "", 3).state()).isEqualTo(HealthState.UP);
    }

    @Test
    void estadoDelNodoB() {
        assertThat(NodeWatchdog.nodeBState(List.of(HealthState.DOWN, HealthState.DOWN))).isEqualTo("SIN_RESPUESTA");
        assertThat(NodeWatchdog.nodeBState(List.of(HealthState.UP, HealthState.DEGRADED))).isEqualTo("DEGRADADO");
        assertThat(NodeWatchdog.nodeBState(List.of(HealthState.UP, HealthState.UP))).isEqualTo("OPERATIVO");
        assertThat(NodeWatchdog.nodeBState(List.of(HealthState.UNKNOWN))).isEqualTo("DESCONOCIDO");
    }
}
