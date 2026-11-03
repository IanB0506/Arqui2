package gt.umg.centinela.correlation.engine;

import gt.umg.centinela.common.model.Evidence;
import gt.umg.centinela.common.model.NormalizedEvent;
import gt.umg.centinela.common.model.Severity;
import gt.umg.centinela.correlation.config.RuleDefinition;

import java.time.Instant;
import java.util.List;

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
