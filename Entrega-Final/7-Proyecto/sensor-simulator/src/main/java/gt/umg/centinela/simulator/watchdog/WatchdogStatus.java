package gt.umg.centinela.simulator.watchdog;

import java.time.Instant;
import java.util.List;

public record WatchdogStatus(String node, boolean enabled, Instant checkedAt, NodeBStatus nodeB,
                             List<TargetStatus> targets, KafkaStatus kafka, List<Transition> transitions) {
    public record Observation(HealthState state, Long latencyMs, String detail) {
    }

    public record TargetStatus(String name, String url, HealthState state, Instant since, Instant lastCheck,
                               Long latencyMs, String detail, int consecutiveFailures) {
    }

    public record Transition(Instant at, String target, HealthState from, HealthState to, String detail,
                             Long previousDurationSeconds) {
    }

    public record NodeBStatus(String state, Instant since, String summary) {
    }

    public record GroupStatus(String group, String state, int members, List<String> consumers, Long lag, String trend,
                              String condition, String message) {
    }

    public record KafkaStatus(HealthState state, Instant since, Integer brokers, String detail,
                              List<GroupStatus> groups) {
    }
}
