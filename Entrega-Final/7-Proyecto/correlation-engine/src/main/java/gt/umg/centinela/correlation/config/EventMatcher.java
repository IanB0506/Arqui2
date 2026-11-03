package gt.umg.centinela.correlation.config;

import gt.umg.centinela.common.model.NormalizedEvent;
import gt.umg.centinela.common.model.SensorType;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class EventMatcher {
    private String label = "";
    private List<SensorType> sensorTypes = new ArrayList<>();
    private List<String> subtypes = new ArrayList<>();
    private Boolean anomalous;
    private Double minConfidence;
    private boolean watchlist;
    private double weight;
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
