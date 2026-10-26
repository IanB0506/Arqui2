package gt.umg.centinela.simulator.scenario;

import gt.umg.centinela.simulator.admin.AlertServiceAdminClient;
import gt.umg.centinela.simulator.config.SimulatorProperties;
import gt.umg.centinela.simulator.generator.BaselineGenerator;
import gt.umg.centinela.simulator.generator.EventFactory;
import gt.umg.centinela.simulator.generator.QuietZones;
import gt.umg.centinela.simulator.publish.EventPublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.scheduling.TaskScheduler;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Cada escenario corre aislado del ruido de fondo: su resultado no depende del azar. */
class ScenarioIsolationTest {

    private ScenarioCatalog catalog;
    private QuietZones quiet;
    private AlertServiceAdminClient correlation;
    private ScenarioRunner runner;

    @BeforeEach
    void setUp() {
        SimulatorProperties properties = new SimulatorProperties();
        catalog = new ScenarioCatalog(new EventFactory(properties), mock(EventPublisher.class),
                mock(BaselineGenerator.class), properties);
        quiet = new QuietZones();
        correlation = mock(AlertServiceAdminClient.class);
        runner = new ScenarioRunner(catalog, mock(TaskScheduler.class), quiet, correlation);
    }

    @Test
    void elRoboAislaSuZonaYLaRutaDeFuga() {
        ScenarioPlan plan = catalog.find("robo-en-curso").orElseThrow().planner().apply(4);
        assertThat(plan.zones()).containsExactly(4, 8, 12);
        assertThat(plan.expectedZones()).containsExactly(4, 8);
    }

    @Test
    void laTabla24AislaLasZonas4y10AunqueSePidaOtraZona() {
        ScenarioExecution execution = runner.run("tabla-2-4", 7, 1.0);
        assertThat(execution.zoneId()).isEqualTo(4);
        assertThat(execution.quietZones()).containsExactly(4, 10);
        assertThat(quiet.isQuiet(10)).isTrue();
        assertThat(quiet.isQuiet(7)).isFalse();
        // aislada hasta que vence la ventana posterior mas larga (5 min) despues del ultimo paso
        assertThat(Duration.between(execution.startedAt(), execution.quietUntil()))
                .isGreaterThanOrEqualTo(ScenarioRunner.QUIET_AFTER);
        verify(correlation).resetZones(List.of(4, 10));
        assertThat(execution.isolation()).contains("incidentes abiertos de 4, 10 reiniciados");
    }

    @Test
    void siElAlertServiceNoRespondeElEscenarioCorreIgual() {
        when(correlation.resetZones(anyCollection())).thenThrow(new IllegalStateException("sin conexion"));
        ScenarioExecution execution = runner.run("falso-positivo", null, 1.0);
        assertThat(execution.zoneId()).isEqualTo(10);
        assertThat(execution.steps()).hasSize(1);
        assertThat(quiet.isQuiet(10)).isTrue();
        assertThat(execution.isolation()).contains("no se pudo reiniciar");
    }
}
