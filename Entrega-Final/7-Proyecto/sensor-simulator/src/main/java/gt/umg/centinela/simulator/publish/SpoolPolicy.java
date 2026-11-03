package gt.umg.centinela.simulator.publish;

import gt.umg.centinela.common.model.raw.AcousticSensorEvent;
import gt.umg.centinela.common.model.raw.LprCameraEvent;
import gt.umg.centinela.common.model.raw.SoundType;
import gt.umg.centinela.common.model.raw.SpeedSensorEvent;
import gt.umg.centinela.simulator.config.SimulatorProperties;
import gt.umg.centinela.simulator.generator.SimulatedEvent;
import org.springframework.stereotype.Component;

@Component
// Si Kafka no acepta: los críticos van al spool, la telemetría útil se guarda 15 min y las lecturas normales se descartan.
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
        return Decision.TELEMETRY;
    }
}
