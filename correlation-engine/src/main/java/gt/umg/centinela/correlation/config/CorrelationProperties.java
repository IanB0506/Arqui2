package gt.umg.centinela.correlation.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** Configuracion del motor de correlacion (prefijo {@code correlation} en application.yml). */
@ConfigurationProperties(prefix = "correlation")
public class CorrelationProperties {

    /** Cuanto tiempo se guardan eventos en las ventanas de Redis (debe cubrir la ventana mas larga). */
    private Duration windowRetention = Duration.ofMinutes(15);
    /** Tiempo que se espera corroboracion antes de clasificar un ancla como posible falso positivo. */
    private Duration falsePositiveTimeout = Duration.ofSeconds(90);
    /** Durante este tiempo, nuevas correlaciones del mismo tipo en la misma zona actualizan el mismo incidente. */
    private Duration incidentCooldown = Duration.ofMinutes(10);
    /** Tiempo durante el que se sigue una placa sospechosa entre zonas. */
    private Duration suspectTracking = Duration.ofMinutes(30);
    /** Si una particion tiene mas mensajes en espera que esto, el motor esta poniendose al dia tras una caida. */
    private long catchUpLag = 50;
    /** Mientras dure la puesta al dia (y este tiempo despues) no se clasifican falsos positivos. */
    private Duration catchUpHold = Duration.ofSeconds(40);
    private DeviceAbuse deviceAbuse = new DeviceAbuse();
    private List<RuleDefinition> rules = new ArrayList<>();

    /** Deteccion de dispositivos que abusan del boton de panico (mitigacion de falsos positivos). */
    public static class DeviceAbuse {
        private int maxPanics = 5;
        private Duration window = Duration.ofMinutes(10);
        private Duration flagDuration = Duration.ofHours(1);

        public int getMaxPanics() {
            return maxPanics;
        }

        public void setMaxPanics(int maxPanics) {
            this.maxPanics = maxPanics;
        }

        public Duration getWindow() {
            return window;
        }

        public void setWindow(Duration window) {
            this.window = window;
        }

        public Duration getFlagDuration() {
            return flagDuration;
        }

        public void setFlagDuration(Duration flagDuration) {
            this.flagDuration = flagDuration;
        }
    }

    public Duration getWindowRetention() {
        return windowRetention;
    }

    public void setWindowRetention(Duration windowRetention) {
        this.windowRetention = windowRetention;
    }

    public Duration getFalsePositiveTimeout() {
        return falsePositiveTimeout;
    }

    public void setFalsePositiveTimeout(Duration falsePositiveTimeout) {
        this.falsePositiveTimeout = falsePositiveTimeout;
    }

    public Duration getIncidentCooldown() {
        return incidentCooldown;
    }

    public void setIncidentCooldown(Duration incidentCooldown) {
        this.incidentCooldown = incidentCooldown;
    }

    public Duration getSuspectTracking() {
        return suspectTracking;
    }

    public void setSuspectTracking(Duration suspectTracking) {
        this.suspectTracking = suspectTracking;
    }

    public long getCatchUpLag() {
        return catchUpLag;
    }

    public void setCatchUpLag(long catchUpLag) {
        this.catchUpLag = catchUpLag;
    }

    public Duration getCatchUpHold() {
        return catchUpHold;
    }

    public void setCatchUpHold(Duration catchUpHold) {
        this.catchUpHold = catchUpHold;
    }

    public DeviceAbuse getDeviceAbuse() {
        return deviceAbuse;
    }

    public void setDeviceAbuse(DeviceAbuse deviceAbuse) {
        this.deviceAbuse = deviceAbuse;
    }

    public List<RuleDefinition> getRules() {
        return rules;
    }

    public void setRules(List<RuleDefinition> rules) {
        this.rules = rules;
    }
}
