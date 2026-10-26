package gt.umg.centinela.correlation.engine;

import gt.umg.centinela.common.model.Evidence;
import gt.umg.centinela.common.model.NormalizedEvent;
import gt.umg.centinela.common.model.SensorType;
import gt.umg.centinela.correlation.config.EventMatcher;
import gt.umg.centinela.correlation.config.RuleDefinition;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Calcula el puntaje de una regla para un ancla dado el conjunto de eventos candidatos de la zona.
 * Funcion pura (sin Redis ni Kafka) para que el modelo de inferencia pueda probarse de forma aislada.
 */
public final class RuleEvaluator {

    private RuleEvaluator() {
    }

    public static RuleMatch evaluate(RuleDefinition rule, NormalizedEvent anchor, Collection<NormalizedEvent> candidates,
                                     Predicate<String> isWatchlisted, double thresholdAdjustment) {
        Instant from = anchor.eventTime().minus(rule.getWindowBefore());
        Instant to = anchor.eventTime().plus(rule.getWindowAfter());

        double score = rule.getAnchor().getWeight();
        List<Evidence> evidence = new ArrayList<>();
        List<String> reasons = new ArrayList<>();
        evidence.add(Evidence.of(anchor, rule.getAnchor().getWeight()));
        reasons.add(String.format(Locale.ROOT, "%s (%s, +%.2f)", rule.getAnchor().getLabel(), anchor.summary(),
                rule.getAnchor().getWeight()));

        // Cada evento suma en una sola evidencia, y dentro de una evidencia cada fuente cuenta una vez:
        // una misma persona o sensor no puede corroborarse a si mismo.
        Set<java.util.UUID> usedEvents = new HashSet<>();

        List<NormalizedEvent> ordered = candidates.stream()
                .filter(e -> !e.eventId().equals(anchor.eventId()))
                .filter(e -> e.zoneId() == anchor.zoneId())
                .filter(e -> !e.eventTime().isBefore(from) && !e.eventTime().isAfter(to))
                .sorted(Comparator.comparing(NormalizedEvent::eventTime))
                .toList();

        String plateHash = anchor.sensorType() == SensorType.LPR ? anchor.plateHash() : null;
        String plateMasked = anchor.sensorType() == SensorType.LPR ? anchor.plateMasked() : null;
        Instant first = anchor.eventTime();

        for (EventMatcher matcher : rule.getEvidence()) {
            int count = 0;
            Set<String> sources = new HashSet<>();
            sources.add(sourceOf(anchor));
            for (NormalizedEvent candidate : ordered) {
                if (count >= matcher.getMaxCount()) {
                    break;
                }
                if (usedEvents.contains(candidate.eventId()) || !matcher.matches(candidate, isWatchlisted)) {
                    continue;
                }
                // Si el ancla es una placa, una camara que identifica OTRA placa es otro vehiculo y no la corrobora.
                // Un sensor de velocidad no lee placas: puede ser el mismo vehiculo y si suma.
                if (anchor.sensorType() == SensorType.LPR && anchor.plateHash() != null
                        && candidate.plateHash() != null && !candidate.plateHash().equals(anchor.plateHash())) {
                    continue;
                }
                if (!sources.add(sourceOf(candidate))) {
                    continue;
                }
                usedEvents.add(candidate.eventId());
                count++;
                score += matcher.getWeight();
                evidence.add(Evidence.of(candidate, matcher.getWeight()));
                reasons.add(String.format(Locale.ROOT, "%s (%s, +%.2f)", matcher.getLabel(), candidate.summary(),
                        matcher.getWeight()));
                if (candidate.eventTime().isBefore(first)) {
                    first = candidate.eventTime();
                }
                if (plateHash == null && candidate.plateHash() != null) {
                    plateHash = candidate.plateHash();
                    plateMasked = candidate.plateMasked();
                }
            }
        }
        score = Math.min(1.0, Math.round(score * 1000) / 1000.0);
        double threshold = Math.min(0.95, rule.getThreshold() + thresholdAdjustment);
        return new RuleMatch(rule, anchor, score, threshold, rule.severityFor(score), evidence, reasons,
                plateHash, plateMasked, first);
    }

    /** Identidad de la fuente: el ciudadano (seudonimo) si existe; si no, el sensor. */
    static String sourceOf(NormalizedEvent event) {
        return event.sourceHash() != null ? event.sourceHash() : event.sensorId();
    }
}
