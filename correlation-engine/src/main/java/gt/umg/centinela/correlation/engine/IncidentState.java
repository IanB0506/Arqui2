package gt.umg.centinela.correlation.engine;

import gt.umg.centinela.common.model.Severity;

import java.util.UUID;

/** Estado de un incidente abierto (para agrupar correlaciones y detectar escalamientos). */
public record IncidentState(UUID alertId, int revision, Severity severity, double score, String plateHash) {
}
