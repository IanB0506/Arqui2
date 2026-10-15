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

/** Configuracion del simulador (application.yml, prefijo {@code simulator}). */
@ConfigurationProperties(prefix = "simulator")
public class SimulatorProperties {

    /** Nombre del nodo que ejecuta esta instancia (aparece en metricas y logs). */
    private String nodeName = "nodo-c";
    /** Zonas que simula esta instancia. Permite repartir la ciudad entre varias maquinas. */
    private List<Integer> zones = IntStream.rangeClosed(1, 12).boxed().toList();
    /** Si genera trafico de fondo continuo al arrancar. */
    private boolean baselineEnabled = true;
    /** Eventos por segundo POR ZONA para cada tipo de sensor (trafico de fondo). */
    private Map<SensorType, Double> ratesPerZone = defaultRates();
    /** Probabilidad de que una lectura de velocidad de fondo sea un exceso de velocidad. */
    private double speedAnomalyProbability = 0.001;
    /** Probabilidad de que una lectura LPR de fondo registre un vehiculo a alta velocidad. */
    private double lprAnomalyProbability = 0.001;
    /** Placas de vehiculos reportados como robados (se usan en el escenario "vehiculo-robado"). */
    private List<String> watchlistPlates = List.of("P512KLM", "P777HTR", "M045BBC");
    /** Carpeta donde se guardan los eventos criticos que no pudieron enviarse a Kafka. */
    private String spoolDir = "./spool";
    /** URL del alert-service, usada por la validacion automatica de correlaciones. */
    private String alertServiceUrl = "http://localhost:8083";
    private String alertServiceUser = "operador";
    private String alertServicePassword = "operador123";
    /** Credenciales de administrador: la validacion las usa para limpiar el estado de correlacion. */
    private String alertServiceAdminUser = "admin";
    private String alertServiceAdminPassword = "admin123";
    /** Timeout de conexion y lectura de las llamadas al alert-service (validacion). */
    private Duration httpTimeout = Duration.ofSeconds(10);
    /** Spool en disco: criticos con escritura anticipada y telemetria util acotada (ADR-006). */
    private Spool spool = new Spool();
    /** Vigilante de los servicios del nodo B y de Kafka. */
    private Watchdog watchdog = new Watchdog();

    /** Configuracion de los dos spools en disco. */
    public static class Spool {
        /** Eventos por lote al reenviar. */
        private int resendBatch = 500;
        /** Espera maxima de las confirmaciones de un lote. */
        private Duration resendTimeout = Duration.ofSeconds(5);
        /** Fallas no transitorias antes de apartar un evento a .rejected (las transitorias no cuentan). */
        private int maxAttempts = 5;
        /** fsync de cada evento critico antes de enviarlo (escrituras agrupadas). */
        private boolean criticalFsync = true;
        /** Tope de criticos guardados; si se llena se rechaza lo nuevo (nunca se borra un critico). */
        private DataSize criticalMaxSize = DataSize.ofMegabytes(32);
        /** Sonidos acusticos que se tratan como criticos (perdida cero). */
        private Set<SoundType> criticalSoundTypes = EnumSet.of(SoundType.DISPARO, SoundType.EXPLOSION);
        /** Probabilidad minima para que un sonido sea critico (mismo umbral de anomalia de la ingesta). */
        private double acousticMinProbability = 0.5;
        /** Guarda la telemetria util cuando Kafka falla (false = se descarta, comportamiento anterior). */
        private boolean telemetryEnabled = true;
        /** Vencimiento de la telemetria guardada: mas vieja ya no sirve para correlacionar. */
        private Duration telemetryMaxAge = Duration.ofMinutes(15);
        /** Tope de telemetria guardada; si se llena se descarta lo mas viejo. */
        private DataSize telemetryMaxSize = DataSize.ofMegabytes(32);
        /** Exceso de velocidad util: velocidad > limite x (1 + tolerancia). */
        private double speedTolerance = 0.3;
        /** Ritmo maximo (eventos/s) al drenar la telemetria, para no simular una sobrecarga. */
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

    /** Configuracion del vigilante (corre en los nodos generadores, fuera del nodo B). */
    public static class Watchdog {
        private boolean enabled = true;
        /** Timeout de cada sonda HTTP y de las consultas a Kafka. */
        private Duration timeout = Duration.ofSeconds(3);
        /** Fallas consecutivas antes de declarar un servicio caido (evita falsas alarmas). */
        private int failuresToDown = 2;
        /** Servicios del nodo B a vigilar: nombre -> URL de salud. Una URL vacia desactiva el objetivo. */
        private Map<String, String> targets = new LinkedHashMap<>();
        /** Grupos de consumidores de los que se informan miembros, nodos y atraso (lag). */
        private List<String> kafkaGroups = List.of("ingestion", "correlation", "event-store", "alert-store");
        /** Atraso desde el que un grupo se marca como ATRASADO. */
        private long lagWarn = 1000;
        /** Transiciones que se guardan para la API y la pagina. */
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
