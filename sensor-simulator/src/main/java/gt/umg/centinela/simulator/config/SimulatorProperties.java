package gt.umg.centinela.simulator.config;

import gt.umg.centinela.common.model.SensorType;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
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

    public String getAlertServicePassword() {
        return alertServicePassword;
    }

    public void setAlertServicePassword(String alertServicePassword) {
        this.alertServicePassword = alertServicePassword;
    }
}
