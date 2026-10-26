package gt.umg.centinela.alerts.live;

import gt.umg.centinela.alerts.persistence.AlertRepository;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

/**
 * Alertas generadas por el propio sistema (incumplimientos de SLO): se guardan como cualquier alerta y se
 * empujan a las consolas abiertas, para que el operador no tenga que recargar la pagina para verlas.
 */
@Component
public class SystemAlerts {

    public static final String TYPE = "SLO_INCUMPLIDO";

    private final AlertRepository alerts;
    private final AlertBroadcaster broadcaster;

    public SystemAlerts(AlertRepository alerts, AlertBroadcaster broadcaster) {
        this.alerts = alerts;
        this.broadcaster = broadcaster;
    }

    public void notify(String severity, String title, String description, Instant at) {
        UUID id = UUID.randomUUID();
        alerts.saveSystemAlert(id, TYPE, severity, title, description, at);
        alerts.findById(id).ifPresent(broadcaster::broadcast);
    }
}
