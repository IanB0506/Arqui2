package gt.umg.centinela.simulator.publish;

import gt.umg.centinela.common.model.raw.AcousticSensorEvent;
import gt.umg.centinela.common.model.raw.LprCameraEvent;
import gt.umg.centinela.common.model.raw.SoundType;
import gt.umg.centinela.common.model.raw.SpeedSensorEvent;
import gt.umg.centinela.simulator.config.SimulatorProperties;
import gt.umg.centinela.simulator.generator.SimulatedEvent;
import org.springframework.stereotype.Component;

/**
 * Decide que hacer con un evento si Kafka no lo acepta (ADR-006, politica de perdida aceptable):
 * <ul>
 *   <li>CRITICAL: panico, reporte ciudadano y disparo/explosion acusticos. Perdida cero: se escriben
 *       en disco ANTES de enviarlos.</li>
 *   <li>TELEMETRY: lecturas que pueden corroborar una alerta (placas, excesos de velocidad, sonidos
 *       relevantes). Se guardan acotadas por tamano y antiguedad: sin ellas, un robo ocurrido durante
 *       una caida se clasificaria como posible falso positivo.</li>
 *   <li>DISCARD: lecturas normales que no participan en ninguna regla (el mismo "filtrado temprano"
 *       que hace el motor de correlacion).</li>
 * </ul>
 */
@Component
public class SpoolPolicy {

    public enum Decision { CRITICAL, TELEMETRY, DISCARD }

    private static final int DEFAULT_SPEED_LIMIT = 60;

    private final SimulatorProperties.Spool config;

    public SpoolPolicy(SimulatorProperties properties) {
        this.config = properties.getSpool();
    }

    public Decision classify(SimulatedEvent event) {
        Decision decision = switch (event.type()) {
            case PANIC, CITIZEN_REPORT -> Decision.CRITICAL;
            case ACOUSTIC -> acoustic((AcousticSensorEvent) event.payload());
            case SPEED -> speed((SpeedSensorEvent) event.payload());
            case LPR -> lpr((LprCameraEvent) event.payload());
        };
        if (decision == Decision.TELEMETRY && !config.isTelemetryEnabled()) {
            return Decision.DISCARD;
        }
        return decision;
    }

    private Decision acoustic(AcousticSensorEvent e) {
        double probability = e.criticalProbability() == null ? 0 : e.criticalProbability();
        if (config.getCriticalSoundTypes().contains(e.soundType()) && probability >= config.getAcousticMinProbability()) {
            return Decision.CRITICAL;
        }
        return e.soundType() != SoundType.RUIDO_AMBIENTAL && probability >= config.getAcousticMinProbability()
                ? Decision.TELEMETRY : Decision.DISCARD;
    }

    private Decision speed(SpeedSensorEvent e) {
        int limit = e.speedLimit() == null ? DEFAULT_SPEED_LIMIT : e.speedLimit();
        boolean excess = e.speed() != null && e.speed() > limit * (1 + config.getSpeedTolerance());
        return excess || Boolean.TRUE.equals(e.wrongWay()) ? Decision.TELEMETRY : Decision.DISCARD;
    }

    private Decision lpr(LprCameraEvent e) {
        // Cualquier placa puede estar en la lista de robados o pertenecer a un vehiculo en fuga, y esas
        // listas solo existen en el centro: el borde no puede filtrar por placa.
        return Decision.TELEMETRY;
    }
}
