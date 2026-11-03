package gt.umg.centinela.simulator.scenario;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.function.IntFunction;

public record ScenarioDefinition(
        String id,
        String title,
        String description,
        int defaultZone,
        boolean includeInValidation,
        boolean fixedZone,
        @JsonIgnore IntFunction<ScenarioPlan> planner) {
}
