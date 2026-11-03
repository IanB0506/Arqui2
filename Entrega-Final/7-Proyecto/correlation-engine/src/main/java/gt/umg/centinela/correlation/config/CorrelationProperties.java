package gt.umg.centinela.correlation.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@ConfigurationProperties(prefix = "correlation")
public class CorrelationProperties {
    private Duration windowRetention = Duration.ofMinutes(15);
    private Duration falsePositiveTimeout = Duration.ofSeconds(90);
    private Duration incidentCooldown = Duration.ofMinutes(10);
    private Duration suspectTracking = Duration.ofMinutes(30);
    private long catchUpLag = 50;
    private Duration catchUpHold = Duration.ofSeconds(40);
    private DeviceAbuse deviceAbuse = new DeviceAbuse();
    private List<RuleDefinition> rules = new ArrayList<>();

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
