package gt.umg.centinela.correlation.engine;

import gt.umg.centinela.common.model.NormalizedEvent;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/** Estado en memoria para pruebas (ignora los TTL). */
class InMemoryCorrelationState implements CorrelationState {

    private final Map<String, IncidentState> incidents = new HashMap<>();
    final Set<UUID> corroborated = new HashSet<>();
    private final Map<String, List<Map.Entry<Instant, NormalizedEvent>>> pending = new HashMap<>();
    private final Map<String, Long> devicePanics = new HashMap<>();
    private final Set<String> flagged = new HashSet<>();
    private final Map<String, SuspectState> suspects = new HashMap<>();
    private final Set<String> sightings = new HashSet<>();
    final Set<String> watchlist = new HashSet<>();
    final Map<String, Double> adjustments = new HashMap<>();

    @Override
    public Optional<IncidentState> incident(String key) {
        return Optional.ofNullable(incidents.get(key));
    }

    @Override
    public void saveIncident(String key, IncidentState state, Duration ttl) {
        if (state == null) {
            incidents.remove(key); // simula la expiracion del TTL
            return;
        }
        incidents.put(key, state);
    }

    @Override
    public void markCorroborated(UUID eventId, Duration ttl) {
        corroborated.add(eventId);
    }

    @Override
    public boolean isCorroborated(UUID eventId) {
        return corroborated.contains(eventId);
    }

    @Override
    public void addPending(String ruleId, NormalizedEvent anchor, Instant deadline) {
        pending.computeIfAbsent(ruleId, k -> new ArrayList<>()).add(Map.entry(deadline, anchor));
    }

    @Override
    public List<NormalizedEvent> claimExpiredPending(String ruleId, Instant now, int limit) {
        List<Map.Entry<Instant, NormalizedEvent>> list = pending.getOrDefault(ruleId, new ArrayList<>());
        List<NormalizedEvent> claimed = list.stream().filter(e -> !e.getKey().isAfter(now)).map(Map.Entry::getValue).toList();
        list.removeIf(e -> !e.getKey().isAfter(now));
        return claimed;
    }

    @Override
    public long incrementDevicePanics(String sensorId, Duration window) {
        return devicePanics.merge(sensorId, 1L, Long::sum);
    }

    @Override
    public boolean flagDevice(String sensorId, Duration ttl) {
        return flagged.add(sensorId);
    }

    @Override
    public boolean isDeviceFlagged(String sensorId) {
        return flagged.contains(sensorId);
    }

    @Override
    public void registerSuspect(String plateHash, SuspectState suspect, Duration ttl) {
        suspects.put(plateHash, suspect);
    }

    @Override
    public Optional<SuspectState> suspect(String plateHash) {
        return Optional.ofNullable(suspects.get(plateHash));
    }

    @Override
    public boolean firstSuspectSightingInZone(UUID alertId, int zoneId, Duration ttl) {
        return sightings.add(alertId + ":" + zoneId);
    }

    @Override
    public boolean suspectSeenInZone(UUID alertId, int zoneId) {
        return sightings.contains(alertId + ":" + zoneId);
    }

    boolean backlogged;

    @Override
    public void markBacklog(Duration ttl) {
        backlogged = true;
    }

    @Override
    public boolean isBacklogged() {
        return backlogged;
    }

    @Override
    public boolean isWatchlisted(String plateHash) {
        return watchlist.contains(plateHash);
    }

    @Override
    public Map<String, Double> thresholdAdjustments() {
        return adjustments;
    }
}
