package gt.umg.centinela.correlation.engine;

import gt.umg.centinela.common.model.Severity;

import java.util.List;
import java.util.UUID;

public record IncidentState(UUID alertId, int revision, Severity severity, double score, String plateHash,
                            List<UUID> anchorIds) {
    public boolean hasAnchor(UUID anchorId) {
        return anchorIds != null && anchorIds.contains(anchorId);
    }
}
