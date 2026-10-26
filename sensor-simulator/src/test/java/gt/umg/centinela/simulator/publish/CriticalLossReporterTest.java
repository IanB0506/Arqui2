package gt.umg.centinela.simulator.publish;

import gt.umg.centinela.simulator.admin.AlertServiceAdminClient;
import gt.umg.centinela.simulator.config.SimulatorProperties;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** SLO-2: un evento critico perdido termina en una alerta al operador, aunque el alert-service tarde en volver. */
class CriticalLossReporterTest {

    private final SimpleMeterRegistry metrics = new SimpleMeterRegistry();
    private final AlertServiceAdminClient admin = mock(AlertServiceAdminClient.class);
    private final CriticalLossReporter reporter = new CriticalLossReporter(metrics, admin, new SimulatorProperties());

    private void lose(String type) {
        metrics.counter("simulator.events.sent", "sensor_type", type, "result", "lost").increment();
    }

    @Test
    void sinPerdidasNoSeInformaNada() {
        metrics.counter("simulator.events.sent", "sensor_type", "PANIC", "result", "ok").increment();
        reporter.report();
        verify(admin, never()).reportLostCriticalEvents(anyString(), anyLong());
    }

    @Test
    void lasPerdidasSeInformanUnaSolaVez() {
        lose("PANIC");
        lose("CITIZEN_REPORT");
        reporter.report();
        reporter.report();
        verify(admin, times(1)).reportLostCriticalEvents("nodo-c", 2);
    }

    @Test
    void siElAlertServiceNoRespondeSeInformaAlVolver() {
        doThrow(new IllegalStateException("nodo B caido")).when(admin).reportLostCriticalEvents("nodo-c", 1);
        lose("PANIC");
        reporter.report();
        lose("ACOUSTIC");
        reporter.report(); // ya responde: informa las dos
        verify(admin).reportLostCriticalEvents("nodo-c", 2);
    }
}
