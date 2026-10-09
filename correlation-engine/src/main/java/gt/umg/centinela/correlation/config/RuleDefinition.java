package gt.umg.centinela.correlation.config;

import gt.umg.centinela.common.model.SensorType;
import gt.umg.centinela.common.model.Severity;

import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * Regla de correlacion por evidencia ponderada.
 *
 * <p>Una regla tiene un evento <b>ancla</b> (el que inicia la sospecha, p. ej. un boton de panico) y
 * una lista de <b>evidencias</b> que la refuerzan si ocurren en la misma zona dentro de la ventana
 * [ancla - windowBefore, ancla + windowAfter]. Puntaje = peso del ancla + suma de pesos de las
 * evidencias encontradas (maximo 1.0). Si el puntaje alcanza el umbral se genera la alerta; si
 * ademas alcanza el umbral critico, la severidad sube a CRITICAL.
 */
public class RuleDefinition {

    /** Alcance del incidente: una alerta por zona (se agrupan) o una por cada evento ancla. */
    public enum IncidentScope { ZONE, ANCHOR }

    private String id;
    private String alertType;
    private String title;
    private Duration windowBefore = Duration.ofMinutes(2);
    private Duration windowAfter = Duration.ofMinutes(5);
    private double threshold = 0.7;
    private Double criticalThreshold;
    private Severity severity = Severity.HIGH;
    /** Si el ancla no se corrobora a tiempo, se emite POSIBLE_FALSO_POSITIVO. */
    private boolean falsePositiveCheck;
    /** Si la alerta identifica una placa, se registra como sospechosa para seguirla entre zonas. */
    private boolean trackPlate;
    private IncidentScope incidentScope = IncidentScope.ZONE;
    private EventMatcher anchor;
    private List<EventMatcher> evidence = new ArrayList<>();

    /** Tipos de sensor que pueden disparar una evaluacion de esta regla. */
    public Set<SensorType> involvedTypes() {
        Set<SensorType> types = EnumSet.noneOf(SensorType.class);
        types.addAll(anchor.getSensorTypes());
        evidence.forEach(e -> types.addAll(e.getSensorTypes()));
        return types;
    }

    public Severity severityFor(double score) {
        return criticalThreshold != null && score >= criticalThreshold - 1e-9 ? Severity.CRITICAL : severity;
    }

    public void validate() {
        if (id == null || alertType == null || anchor == null) {
            throw new IllegalStateException("Regla incompleta: id, alert-type y anchor son obligatorios (" + id + ")");
        }
        if (threshold <= 0 || threshold > 1 || (criticalThreshold != null && criticalThreshold < threshold)) {
            throw new IllegalStateException("Umbrales invalidos en la regla " + id);
        }
        if (anchor.getSensorTypes().isEmpty()) {
            throw new IllegalStateException("El ancla de la regla " + id + " no tiene tipos de sensor");
        }
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getAlertType() {
        return alertType;
    }

    public void setAlertType(String alertType) {
        this.alertType = alertType;
    }

    public String getTitle() {
        return title == null ? alertType : title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Duration getWindowBefore() {
        return windowBefore;
    }

    public void setWindowBefore(Duration windowBefore) {
        this.windowBefore = windowBefore;
    }

    public Duration getWindowAfter() {
        return windowAfter;
    }

    public void setWindowAfter(Duration windowAfter) {
        this.windowAfter = windowAfter;
    }

    public double getThreshold() {
        return threshold;
    }

    public void setThreshold(double threshold) {
        this.threshold = threshold;
    }

    public Double getCriticalThreshold() {
        return criticalThreshold;
    }

    public void setCriticalThreshold(Double criticalThreshold) {
        this.criticalThreshold = criticalThreshold;
    }

    public Severity getSeverity() {
        return severity;
    }

    public void setSeverity(Severity severity) {
        this.severity = severity;
    }

    public boolean isFalsePositiveCheck() {
        return falsePositiveCheck;
    }

    public void setFalsePositiveCheck(boolean falsePositiveCheck) {
        this.falsePositiveCheck = falsePositiveCheck;
    }

    public boolean isTrackPlate() {
        return trackPlate;
    }

    public void setTrackPlate(boolean trackPlate) {
        this.trackPlate = trackPlate;
    }

    public IncidentScope getIncidentScope() {
        return incidentScope;
    }

    public void setIncidentScope(IncidentScope incidentScope) {
        this.incidentScope = incidentScope;
    }

    public EventMatcher getAnchor() {
        return anchor;
    }

    public void setAnchor(EventMatcher anchor) {
        this.anchor = anchor;
    }

    public List<EventMatcher> getEvidence() {
        return evidence;
    }

    public void setEvidence(List<EventMatcher> evidence) {
        this.evidence = evidence;
    }
}
