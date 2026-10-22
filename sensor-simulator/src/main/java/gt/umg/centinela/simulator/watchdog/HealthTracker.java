package gt.umg.centinela.simulator.watchdog;

import gt.umg.centinela.simulator.watchdog.WatchdogStatus.Observation;
import gt.umg.centinela.simulator.watchdog.WatchdogStatus.TargetStatus;
import gt.umg.centinela.simulator.watchdog.WatchdogStatus.Transition;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * Estado de un objetivo con histeresis: UP y DEGRADED se adoptan de inmediato, pero DOWN solo tras
 * {@code failuresToDown} fallas seguidas, para que un corte de red de un segundo no dispare la alarma.
 */
final class HealthTracker {

    private final String name;
    private final String url;
    private final int failuresToDown;
    private HealthState state = HealthState.UNKNOWN;
    private Instant since;
    private Instant lastCheck;
    private Long latencyMs;
    private String detail = "sin revisar todavia";
    private int consecutiveFailures;

    HealthTracker(String name, String url, int failuresToDown) {
        this.name = name;
        this.url = url;
        this.failuresToDown = Math.max(1, failuresToDown);
    }

    synchronized Optional<Transition> observe(Observation o, Instant now) {
        lastCheck = now;
        latencyMs = o.latencyMs();
        detail = o.detail();
        HealthState target = o.state();
        if (target == HealthState.DOWN) {
            consecutiveFailures++;
            if (state != HealthState.UNKNOWN && consecutiveFailures < failuresToDown) {
                return Optional.empty(); // todavia no: se conserva el estado anterior
            }
        } else {
            consecutiveFailures = 0;
        }
        if (target == state) {
            return Optional.empty();
        }
        HealthState from = state;
        Long previous = since == null ? null : Duration.between(since, now).toSeconds();
        state = target;
        since = now;
        return Optional.of(new Transition(now, name, from, target, o.detail(), previous));
    }

    synchronized HealthState state() {
        return state;
    }

    synchronized TargetStatus status() {
        return new TargetStatus(name, url, state, since, lastCheck, latencyMs, detail, consecutiveFailures);
    }
}
