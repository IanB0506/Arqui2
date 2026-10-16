package gt.umg.centinela.correlation.config;

import gt.umg.centinela.common.model.NormalizedEvent;
import gt.umg.centinela.common.model.SensorType;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Condicion que debe cumplir un evento para contar como ancla o evidencia de una regla.
 * Todas las condiciones configuradas deben cumplirse (AND).
 */
public class EventMatcher {

    /** Texto legible que aparece en la explicacion de la alerta. */
    private String label = "";
    /** Tipos de sensor aceptados. */
    private List<SensorType> sensorTypes = new ArrayList<>();
    /** Subtipos aceptados (vacio = cualquiera). */
    private List<String> subtypes = new ArrayList<>();
    /** Si se indica, el evento debe ser (o no) anomalo. */
    private Boolean anomalous;
    /** Confianza minima (probabilidad reportada por el sensor). */
    private Double minConfidence;
    /** Si true, la placa del evento debe estar en la lista de vehiculos robados. */
    private boolean watchlist;
    /** Peso que suma al puntaje cada evento que cumple la condicion. */
    private double weight;
    /** Maximo de eventos distintos que suman peso (p. ej. hasta 2 reportes adicionales). */
    private int maxCount = 1;

    public boolean matches(NormalizedEvent event, Predicate<String> isWatchlisted) {
        if (!sensorTypes.contains(event.sensorType())) {
            return false;
        }
        if (!subtypes.isEmpty() && !subtypes.contains(event.subtype())) {
            return false;
        }
        if (anomalous != null && event.anomalous() != anomalous) {
            return false;
        }
        if (minConfidence != null && event.confidence() < minConfidence) {
            return false;
        }
        return !watchlist || (event.plateHash() != null && isWatchlisted.test(event.plateHash()));
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public List<SensorType> getSensorTypes() {
        return sensorTypes;
    }

    public void setSensorTypes(List<SensorType> sensorTypes) {
        this.sensorTypes = sensorTypes;
    }

    public List<String> getSubtypes() {
        return subtypes;
    }

    public void setSubtypes(List<String> subtypes) {
        this.subtypes = subtypes;
    }

    public Boolean getAnomalous() {
        return anomalous;
    }

    public void setAnomalous(Boolean anomalous) {
        this.anomalous = anomalous;
    }

    public Double getMinConfidence() {
        return minConfidence;
    }

    public void setMinConfidence(Double minConfidence) {
        this.minConfidence = minConfidence;
    }

    public boolean isWatchlist() {
        return watchlist;
    }

    public void setWatchlist(boolean watchlist) {
        this.watchlist = watchlist;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    public int getMaxCount() {
        return maxCount;
    }

    public void setMaxCount(int maxCount) {
        this.maxCount = maxCount;
    }
}
