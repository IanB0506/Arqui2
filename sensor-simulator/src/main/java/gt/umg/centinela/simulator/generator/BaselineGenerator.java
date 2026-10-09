package gt.umg.centinela.simulator.generator;

import gt.umg.centinela.common.model.SensorType;
import gt.umg.centinela.simulator.config.SimulatorProperties;
import gt.umg.centinela.simulator.publish.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Trafico de fondo: emite continuamente eventos de los cinco tipos de sensores segun las tasas
 * configuradas. Cada 100 ms acumula "tasa x tiempo" y emite la parte entera, de modo que se
 * respetan tasas fraccionarias (p. ej. 0.001 eventos/s).
 *
 * <p>El multiplicador permite simular picos de carga (concierto, partido, manifestacion).
 */
@Component
public class BaselineGenerator {

    private static final Logger log = LoggerFactory.getLogger(BaselineGenerator.class);
    private static final double TICK_SECONDS = 0.1;

    private final SimulatorProperties properties;
    private final EventFactory factory;
    private final EventPublisher publisher;
    private final Map<SensorType, Double> accumulators = new EnumMap<>(SensorType.class);

    private volatile boolean enabled;
    private volatile double multiplier = 1.0;
    private volatile Instant burstUntil = Instant.EPOCH;

    public BaselineGenerator(SimulatorProperties properties, EventFactory factory, EventPublisher publisher) {
        this.properties = properties;
        this.factory = factory;
        this.publisher = publisher;
        this.enabled = properties.isBaselineEnabled();
        for (SensorType type : SensorType.values()) {
            accumulators.put(type, ThreadLocalRandom.current().nextDouble());
        }
    }

    @Scheduled(fixedRate = 100)
    public void tick() {
        if (!enabled) {
            return;
        }
        double factor = Instant.now().isBefore(burstUntil) ? multiplier : 1.0;
        List<Integer> zones = properties.getZones();
        for (SensorType type : SensorType.values()) {
            double rate = properties.getRatesPerZone().getOrDefault(type, 0.0) * zones.size() * factor;
            double acc = accumulators.get(type) + rate * TICK_SECONDS;
            int toEmit = (int) acc;
            accumulators.put(type, acc - toEmit);
            for (int i = 0; i < toEmit; i++) {
                int zone = zones.get(ThreadLocalRandom.current().nextInt(zones.size()));
                publisher.publish(randomEvent(type, zone));
            }
        }
    }

    private SimulatedEvent randomEvent(SensorType type, int zone) {
        return switch (type) {
            case PANIC -> factory.randomPanic(zone);
            case LPR -> factory.randomLpr(zone);
            case SPEED -> factory.randomSpeed(zone);
            case ACOUSTIC -> factory.randomAcoustic(zone);
            case CITIZEN_REPORT -> factory.randomCitizen(zone);
        };
    }

    /** Activa un pico de carga: multiplica todas las tasas durante el tiempo indicado. */
    public void startBurst(double multiplier, int seconds) {
        this.multiplier = multiplier;
        this.burstUntil = Instant.now().plusSeconds(seconds);
        log.info("Pico de carga x{} durante {} s", multiplier, seconds);
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
        log.info("Trafico de fondo {}", enabled ? "activado" : "desactivado");
    }

    public boolean isEnabled() {
        return enabled;
    }

    public boolean isBursting() {
        return Instant.now().isBefore(burstUntil);
    }

    public double currentMultiplier() {
        return isBursting() ? multiplier : 1.0;
    }

    /** Eventos por segundo esperados con la configuracion actual. */
    public double expectedEventsPerSecond() {
        double total = properties.getRatesPerZone().values().stream().mapToDouble(Double::doubleValue).sum();
        return total * properties.getZones().size() * currentMultiplier();
    }
}
