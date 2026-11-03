package gt.umg.centinela.simulator.config;

import gt.umg.centinela.common.model.SensorType;
import gt.umg.centinela.common.model.raw.SoundType;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.unit.DataSize;

import java.time.Duration;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

@ConfigurationProperties(prefix = "simulator")
public class SimulatorProperties {
    private String nodeName = "nodo-c";
    private List<Integer> zones = IntStream.rangeClosed(1, 12).boxed().toList();
    private boolean baselineEnabled = true;
    private Map<SensorType, Double> ratesPerZone = defaultRates();
    private double speedAnomalyProbability = 0.001;
    private double lprAnomalyProbability = 0.001;
    private List<String> watchlistPlates = List.of("P512KLM", "P777HTR", "M045BBC");
    private String spoolDir = "./spool";
    private String alertServiceUrl = "http://localhost:8083";
    private String alertServiceUser = "operador";
    private String alertServicePassword = "operador123";
    private String alertServiceAdminUser = "admin";
    private String alertServiceAdminPassword = "admin123";
    private Duration httpTimeout = Duration.ofSeconds(10);
    private Spool spool = new Spool();
    private Watchdog watchdog = new Watchdog();

    public static class Spool {
        private int resendBatch = 500;
        private Duration resendTimeout = Duration.ofSeconds(5);
        private int maxAttempts = 5;
        private boolean criticalFsync = true;
        private DataSize criticalMaxSize = DataSize.ofMegabytes(32);
        private Set<SoundType> criticalSoundTypes = EnumSet.of(SoundType.DISPARO, SoundType.EXPLOSION);
        private double acousticMinProbability = 0.5;
        private boolean telemetryEnabled = true;
        private Duration telemetryMaxAge = Duration.ofMinutes(15);
        private DataSize telemetryMaxSize = DataSize.ofMegabytes(32);
        private double speedTolerance = 0.3;
        private int telemetryResendRate = 500;

        public int getResendBatch() { return resendBatch; }
        public void setResendBatch(int resendBatch) { this.resendBatch = resendBatch; }
        public Duration getResendTimeout() { return resendTimeout; }
        public void setResendTimeout(Duration resendTimeout) { this.resendTimeout = resendTimeout; }
        public int getMaxAttempts() { return maxAttempts; }
        public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
        public boolean isCriticalFsync() { return criticalFsync; }
        public void setCriticalFsync(boolean criticalFsync) { this.criticalFsync = criticalFsync; }
        public DataSize getCriticalMaxSize() { return criticalMaxSize; }
        public void setCriticalMaxSize(DataSize criticalMaxSize) { this.criticalMaxSize = criticalMaxSize; }
        public Set<SoundType> getCriticalSoundTypes() { return criticalSoundTypes; }
        public void setCriticalSoundTypes(Set<SoundType> criticalSoundTypes) { this.criticalSoundTypes = criticalSoundTypes; }
        public double getAcousticMinProbability() { return acousticMinProbability; }
        public void setAcousticMinProbability(double acousticMinProbability) { this.acousticMinProbability = acousticMinProbability; }
        public boolean isTelemetryEnabled() { return telemetryEnabled; }
        public void setTelemetryEnabled(boolean telemetryEnabled) { this.telemetryEnabled = telemetryEnabled; }
        public Duration getTelemetryMaxAge() { return telemetryMaxAge; }
        public void setTelemetryMaxAge(Duration telemetryMaxAge) { this.telemetryMaxAge = telemetryMaxAge; }
        public DataSize getTelemetryMaxSize() { return telemetryMaxSize; }
        public void setTelemetryMaxSize(DataSize telemetryMaxSize) { this.telemetryMaxSize = telemetryMaxSize; }
        public double getSpeedTolerance() { return speedTolerance; }
        public void setSpeedTolerance(double speedTolerance) { this.speedTolerance = speedTolerance; }
        public int getTelemetryResendRate() { return telemetryResendRate; }
        public void setTelemetryResendRate(int telemetryResendRate) { this.telemetryResendRate = telemetryResendRate; }
    }

    public static class Watchdog {
        private boolean enabled = true;
        private Duration timeout = Duration.ofSeconds(3);
        private int failuresToDown = 2;
        private Map<String, String> targets = new LinkedHashMap<>();
        private List<String> kafkaGroups = List.of("ingestion", "correlation", "event-store", "alert-store");
        private long lagWarn = 1000;
        private int historySize = 100;

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public Duration getTimeout() { return timeout; }
        public void setTimeout(Duration timeout) { this.timeout = timeout; }
        public int getFailuresToDown() { return failuresToDown; }
        public void setFailuresToDown(int failuresToDown) { this.failuresToDown = failuresToDown; }
        public Map<String, String> getTargets() { return targets; }
        public void setTargets(Map<String, String> targets) { this.targets = targets; }
        public List<String> getKafkaGroups() { return kafkaGroups; }
        public void setKafkaGroups(List<String> kafkaGroups) { this.kafkaGroups = kafkaGroups; }
        public long getLagWarn() { return lagWarn; }
        public void setLagWarn(long lagWarn) { this.lagWarn = lagWarn; }
        public int getHistorySize() { return historySize; }
        public void setHistorySize(int historySize) { this.historySize = historySize; }
    }

    private static Map<SensorType, Double> defaultRates() {
        Map<SensorType, Double> rates = new EnumMap<>(SensorType.class);
        rates.put(SensorType.PANIC, 0.001);
        rates.put(SensorType.LPR, 0.6);
        rates.put(SensorType.SPEED, 0.5);
        rates.put(SensorType.ACOUSTIC, 0.02);
        rates.put(SensorType.CITIZEN_REPORT, 0.002);
        return rates;
    }

    public String getNodeName() {
        return nodeName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public List<Integer> getZones() {
        return zones;
    }

    public void setZones(List<Integer> zones) {
        this.zones = zones;
    }

    public boolean isBaselineEnabled() {
        return baselineEnabled;
    }

    public void setBaselineEnabled(boolean baselineEnabled) {
        this.baselineEnabled = baselineEnabled;
    }

    public Map<SensorType, Double> getRatesPerZone() {
        return ratesPerZone;
    }

    public void setRatesPerZone(Map<SensorType, Double> ratesPerZone) {
        Map<SensorType, Double> merged = defaultRates();
        merged.putAll(ratesPerZone);
        this.ratesPerZone = merged;
    }

    public double getSpeedAnomalyProbability() {
        return speedAnomalyProbability;
    }

    public void setSpeedAnomalyProbability(double speedAnomalyProbability) {
        this.speedAnomalyProbability = speedAnomalyProbability;
    }

    public double getLprAnomalyProbability() {
        return lprAnomalyProbability;
    }

    public void setLprAnomalyProbability(double lprAnomalyProbability) {
        this.lprAnomalyProbability = lprAnomalyProbability;
    }

    public List<String> getWatchlistPlates() {
        return watchlistPlates;
    }

    public void setWatchlistPlates(List<String> watchlistPlates) {
        this.watchlistPlates = watchlistPlates;
    }

    public String getSpoolDir() {
        return spoolDir;
    }

    public void setSpoolDir(String spoolDir) {
        this.spoolDir = spoolDir;
    }

    public String getAlertServiceUrl() {
        return alertServiceUrl;
    }

    public void setAlertServiceUrl(String alertServiceUrl) {
        this.alertServiceUrl = alertServiceUrl;
    }

    public String getAlertServiceUser() {
        return alertServiceUser;
    }

    public void setAlertServiceUser(String alertServiceUser) {
        this.alertServiceUser = alertServiceUser;
    }

    public String getAlertServiceAdminUser() {
        return alertServiceAdminUser;
    }

    public void setAlertServiceAdminUser(String alertServiceAdminUser) {
        this.alertServiceAdminUser = alertServiceAdminUser;
    }

    public String getAlertServiceAdminPassword() {
        return alertServiceAdminPassword;
    }

    public void setAlertServiceAdminPassword(String alertServiceAdminPassword) {
        this.alertServiceAdminPassword = alertServiceAdminPassword;
    }

    public Duration getHttpTimeout() {
        return httpTimeout;
    }

    public void setHttpTimeout(Duration httpTimeout) {
        this.httpTimeout = httpTimeout;
    }

    public Spool getSpool() {
        return spool;
    }

    public void setSpool(Spool spool) {
        this.spool = spool;
    }

    public Watchdog getWatchdog() {
        return watchdog;
    }

    public void setWatchdog(Watchdog watchdog) {
        this.watchdog = watchdog;
    }

    public String getAlertServicePassword() {
        return alertServicePassword;
    }

    public void setAlertServicePassword(String alertServicePassword) {
        this.alertServicePassword = alertServicePassword;
    }
}
