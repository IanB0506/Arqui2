package gt.umg.centinela.correlation.engine;

import gt.umg.centinela.common.model.Evidence;
import gt.umg.centinela.common.model.NormalizedEvent;
import gt.umg.centinela.common.model.Severity;
import gt.umg.centinela.correlation.config.RuleDefinition;

import java.time.Instant;
import java.util.List;

/**
 * Resultado de evaluar una regla para un ancla.
 *
 * @param rule        regla evaluada
 * @param anchor      evento ancla
 * @param score       puntaje obtenido (0..1)
 * @param threshold   umbral efectivo usado (incluye el ajuste adaptativo de la zona)
 * @param severity    severidad correspondiente al puntaje
 * @param evidence    ancla + evidencias que sumaron puntaje
 * @param reasons     explicacion legible de cada evidencia encontrada
 * @param plateHash   placa sospechosa (primera evidencia LPR), si existe
 * @param plateMasked placa sospechosa enmascarada
 * @param firstEventTime hora del evento mas antiguo de la evidencia
 */
public record RuleMatch(
        RuleDefinition rule,
        NormalizedEvent anchor,
        double score,
        double threshold,
        Severity severity,
        List<Evidence> evidence,
        List<String> reasons,
        String plateHash,
        String plateMasked,
        Instant firstEventTime) {

    public boolean reachedThreshold() {
        return score >= threshold - 1e-9;
    }
}
