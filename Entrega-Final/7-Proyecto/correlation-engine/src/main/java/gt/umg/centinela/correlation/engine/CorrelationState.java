package gt.umg.centinela.correlation.engine;

import gt.umg.centinela.common.model.NormalizedEvent;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public interface CorrelationState {
    Optional<IncidentState> incident(String key);

    void saveIncident(String key, IncidentState state, Duration ttl);

    void markCorroborated(UUID eventId, Duration ttl);

    boolean isCorroborated(UUID eventId);

    void addPending(String ruleId, NormalizedEvent anchor, Instant deadline);

    List<NormalizedEvent> claimExpiredPending(String ruleId, Instant now, int limit);

    long incrementDevicePanics(String sensorId, Duration window);

    boolean flagDevice(String sensorId, Duration ttl);

    boolean isDeviceFlagged(String sensorId);

    void registerSuspect(String plateHash, SuspectState suspect, Duration ttl);

    Optional<SuspectState> suspect(String plateHash);

    boolean firstSuspectSightingInZone(UUID alertId, int zoneId, Duration ttl);

    boolean suspectSeenInZone(UUID alertId, int zoneId);

    void markBacklog(Duration ttl);

    boolean isBacklogged();

    boolean isWatchlisted(String plateHash);

    Map<String, Double> thresholdAdjustments();
}
