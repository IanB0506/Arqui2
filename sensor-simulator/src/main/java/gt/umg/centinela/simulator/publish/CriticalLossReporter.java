package gt.umg.centinela.simulator.publish;

import gt.umg.centinela.simulator.admin.AlertServiceAdminClient;
import gt.umg.centinela.simulator.config.SimulatorProperties;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * SLO-2 (ADR-008): cero eventos criticos perdidos. Si un evento critico no se pudo enviar ni guardar en el spool
 * (Kafka caido y disco lleno o con error), se informa al alert-service, que avisa al operador con una alerta de
 * sistema CRITICAL. Mientras el alert-service no responda (justo lo normal en una caida larga), el conteo se
 * conserva y se informa en cuanto vuelva.
 */
@Component
public class CriticalLossReporter {

    private static final Logger log = LoggerFactory.getLogger(CriticalLossReporter.class);

    private final MeterRegistry metrics;
    private final AlertServiceAdminClient admin;
    private final String node;
    private long reported;

    public CriticalLossReporter(MeterRegistry metrics, AlertServiceAdminClient admin, SimulatorProperties properties) {
        this.metrics = metrics;
        this.admin = admin;
        this.node = properties.getNodeName();
    }

    @Scheduled(fixedDelay = 30_000, initialDelay = 30_000)
    public synchronized void report() {
        long lost = (long) metrics.find("simulator.events.sent").tag("result", "lost").counters().stream()
                .mapToDouble(Counter::count).sum();
        long pending = lost - reported;
        if (pending <= 0) {
            return;
        }
        try {
            admin.reportLostCriticalEvents(node, pending);
            reported = lost;
            log.warn("Se informo al alert-service la perdida de {} evento(s) critico(s) (SLO-2)", pending);
        } catch (RuntimeException e) {
            log.debug("No se pudo informar la perdida de eventos criticos; se reintenta: {}", e.getMessage());
        }
    }
}
