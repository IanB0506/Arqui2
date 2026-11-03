package gt.umg.centinela.correlation.engine;

import gt.umg.centinela.common.model.AlertEvent;
import gt.umg.centinela.common.model.AlertStatus;
import gt.umg.centinela.common.model.Evidence;
import gt.umg.centinela.common.model.NormalizedEvent;
import gt.umg.centinela.common.model.SensorType;
import gt.umg.centinela.common.model.Severity;
import gt.umg.centinela.correlation.config.CorrelationProperties;
import gt.umg.centinela.correlation.config.RuleDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

public class CorrelationEngine {
    private static final Logger log = LoggerFactory.getLogger(CorrelationEngine.class);
    private static final int MAX_ANCHORS_PER_EVALUATION = 5;
    private static final Duration CORROBORATION_MEMORY = Duration.ofHours(1);

    public static final String FALSE_POSITIVE = "POSIBLE_FALSO_POSITIVO";
    public static final String SUSPICIOUS_DEVICE = "DISPOSITIVO_SOSPECHOSO";
    public static final String VEHICLE_ESCAPING = "VEHICULO_EN_FUGA";

    private final CorrelationProperties properties;
    private final WindowStore windows;
    private final CorrelationState state;
    private final Supplier<Map<String, Double>> adjustments;
    private final Clock clock;
    private volatile long lastBacklogMark;
    private volatile long localHoldUntil;
    private final Map<String, Long> failingPartitions = new java.util.concurrent.ConcurrentHashMap<>();
    private static final long FAILING_WINDOW_MS = 60_000;

    public CorrelationEngine(CorrelationProperties properties, WindowStore windows, CorrelationState state,
                             Supplier<Map<String, Double>> adjustments, Clock clock) {
        properties.getRules().forEach(RuleDefinition::validate);
        this.properties = properties;
        this.windows = windows;
        this.state = state;
        this.adjustments = adjustments;
        this.clock = clock;
        this.localHoldUntil = clock.millis() + properties.getCatchUpHold().toMillis();
    }

    public List<RuleDefinition> rules() {
        return properties.getRules();
    }

    public List<AlertEvent> onEvent(NormalizedEvent event) {
        List<AlertEvent> alerts = new ArrayList<>();
        onEvent(event, alerts::add);
        return alerts;
    }

    public void onEvent(NormalizedEvent event, AlertSink sink) {
        if (event.replayedFromSpool()) {
            markCatchUp();
        }
        if (event.sensorType() == SensorType.PANIC) {
            checkDeviceAbuse(event, sink);
        }
        if (event.sensorType() == SensorType.LPR && event.plateHash() != null) {
            checkSuspectVehicle(event, sink);
        }
        List<RuleDefinition> relevant = properties.getRules().stream()
                .filter(rule -> isRelevant(rule, event))
                .toList();
        if (relevant.isEmpty()) {
            return;
        }
        windows.add(event);
        for (RuleDefinition rule : relevant) {
            evaluateRule(rule, event, sink);
        }
    }

    public void noteConsumerLag(long lag) {
        if (lag > properties.getCatchUpLag()) {
            markCatchUp();
        }
    }

    public void notePartitionsAssigned() {
        holdLocally();
    }

    public void noteProcessingFailure(String partition) {
        failingPartitions.put(partition, clock.millis());
    }

    public void noteProcessingOk(String partition) {
        if (!failingPartitions.isEmpty() && failingPartitions.remove(partition) != null) {
            holdLocally();
            try {
                markCatchUp();
            } catch (RuntimeException e) {
                log.debug("No se pudo avisar la puesta al dia a las demas instancias: {}", e.getMessage());
            }
        }
    }

    private void holdLocally() {
        localHoldUntil = Math.max(localHoldUntil, clock.millis() + properties.getCatchUpHold().toMillis());
    }

    private boolean consumersFailing() {
        long now = clock.millis();
        failingPartitions.values().removeIf(at -> now - at > FAILING_WINDOW_MS);
        return !failingPartitions.isEmpty();
    }

    private void markCatchUp() {
        long now = clock.millis();
        if (now - lastBacklogMark > 2_000) {
            lastBacklogMark = now;
            state.markBacklog(properties.getCatchUpHold());
        }
    }

    private boolean isRelevant(RuleDefinition rule, NormalizedEvent event) {
        return rule.getAnchor().matches(event, state::isWatchlisted)
                || rule.getEvidence().stream().anyMatch(m -> m.matches(event, state::isWatchlisted));
    }

    private void evaluateRule(RuleDefinition rule, NormalizedEvent event, AlertSink sink) {
        boolean isAnchor = rule.getAnchor().matches(event, state::isWatchlisted) && !isFlaggedPanic(event);
        boolean isEvidence = rule.getEvidence().stream().anyMatch(m -> m.matches(event, state::isWatchlisted));
        if (!isAnchor && !isEvidence) {
            return;
        }
        List<RuleMatch> matches = new ArrayList<>();
        if (isAnchor) {
            RuleMatch own = evaluate(rule, event);
            if (!own.reachedThreshold() && rule.isFalsePositiveCheck()) {
                state.addPending(rule.getId(), event, clock.instant().plus(properties.getFalsePositiveTimeout()));
            }
            matches.add(own);
        }
        if (isEvidence) {
            for (NormalizedEvent anchor : anchorsAround(rule, event)) {
                matches.add(evaluate(rule, anchor));
            }
        }
        // De mayor a menor puntaje: si la mejor coincidencia no aporta nada al incidente abierto, se prueba la siguiente.
        matches.sort(Comparator.comparingDouble(RuleMatch::score).reversed());
        for (RuleMatch match : matches) {
            if (match.reachedThreshold() && openOrEscalate(match, event.eventTime(), sink)) {
                return;
            }
        }
    }

    private RuleMatch evaluate(RuleDefinition rule, NormalizedEvent anchor) {
        Instant from = anchor.eventTime().minus(rule.getWindowBefore());
        Instant to = anchor.eventTime().plus(rule.getWindowAfter());
        List<NormalizedEvent> candidates = new ArrayList<>();
        for (SensorType type : rule.involvedTypes()) {
            candidates.addAll(windows.range(anchor.zoneId(), type, from, to));
        }
        double adjustment = adjustments.get().getOrDefault(rule.getId() + ":" + anchor.zoneId(), 0.0);
        return RuleEvaluator.evaluate(rule, anchor, candidates, state::isWatchlisted, adjustment);
    }

    private List<NormalizedEvent> anchorsAround(RuleDefinition rule, NormalizedEvent event) {
        Instant from = event.eventTime().minus(rule.getWindowAfter());
        Instant to = event.eventTime().plus(rule.getWindowBefore());
        List<NormalizedEvent> anchors = new ArrayList<>();
        for (SensorType type : rule.getAnchor().getSensorTypes()) {
            windows.range(event.zoneId(), type, from, to).stream()
                    .filter(a -> !a.eventId().equals(event.eventId()))
                    .filter(a -> rule.getAnchor().matches(a, state::isWatchlisted))
                    .filter(a -> !isFlaggedPanic(a))
                    .forEach(anchors::add);
        }
        anchors.sort(Comparator.comparing(NormalizedEvent::eventTime).reversed());
        return anchors.size() > MAX_ANCHORS_PER_EVALUATION ? anchors.subList(0, MAX_ANCHORS_PER_EVALUATION) : anchors;
    }

    private boolean openOrEscalate(RuleMatch match, Instant triggerTime, AlertSink sink) {
        RuleDefinition rule = match.rule();
        String key = rule.getIncidentScope() == RuleDefinition.IncidentScope.ANCHOR
                ? rule.getAlertType() + ":" + match.anchor().eventId()
                : rule.getAlertType() + ":" + match.anchor().zoneId();
        Optional<IncidentState> existing = state.incident(key);
        UUID alertId;
        int revision;
        AlertStatus status;
        Severity severity = match.severity();
        String plateHash = match.plateHash();
        List<UUID> anchors = new ArrayList<>();
        if (existing.isEmpty()) {
            alertId = deterministicId("incident:" + key + ":" + match.anchor().eventId());
            revision = 1;
            status = AlertStatus.NEW;
        } else {
            IncidentState current = existing.get();
            boolean higher = match.severity().isHigherThan(current.severity());
            boolean newPlate = current.plateHash() == null && match.plateHash() != null;
            boolean newAnchor = !current.hasAnchor(match.anchor().eventId());
            if (!higher && !newPlate && !newAnchor) {
                return false;
            }
            alertId = current.alertId();
            revision = current.revision() + 1;
            status = higher || newPlate ? AlertStatus.ESCALATED : AlertStatus.UPDATED;
            if (!higher) {
                severity = current.severity();
            }
            if (plateHash == null) {
                plateHash = current.plateHash();
            }
            if (current.anchorIds() != null) {
                anchors.addAll(current.anchorIds());
            }
        }
        if (!anchors.contains(match.anchor().eventId()) && anchors.size() < 50) {
            anchors.add(match.anchor().eventId());
        }
        String description = String.format(Locale.ROOT, "%s en %s. Puntaje %.2f (umbral %.2f). Evidencia: %s.",
                rule.getTitle(), match.anchor().zoneName(), match.score(), match.threshold(),
                String.join("; ", match.reasons()));
        AlertEvent alert = new AlertEvent(alertId, revision, rule.getId(), rule.getAlertType(), severity, status,
                match.anchor().zoneId(), match.anchor().zoneName(), match.anchor().latitude(), match.anchor().longitude(),
                match.score(), rule.getTitle(), description, clock.instant(), match.firstEventTime(),
                triggerTime, match.evidence(), match.plateMasked(), match.plateHash(), null);
        sink.publish(alert);
        markEvidenceCorroborated(match.evidence());
        state.saveIncident(key, new IncidentState(alertId, revision, severity, match.score(), plateHash, anchors),
                properties.getIncidentCooldown());
        if (rule.isTrackPlate() && match.plateHash() != null) {
            state.registerSuspect(match.plateHash(), new SuspectState(alertId, rule.getAlertType(),
                    match.anchor().zoneId(), clock.instant(), match.plateMasked()), properties.getSuspectTracking());
            state.firstSuspectSightingInZone(alertId, match.anchor().zoneId(), properties.getSuspectTracking());
        }
        log.info("ALERTA {} rev{} {} {} puntaje={} ({})", rule.getAlertType(), revision, severity,
                match.anchor().zoneName(), match.score(), status);
        return true;
    }

    private static UUID deterministicId(String seed) {
        // ID determinista: si otra instancia recalcula la alerta, obtiene el mismo ID y no la duplica.
        return UUID.nameUUIDFromBytes(seed.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private void markEvidenceCorroborated(List<Evidence> evidence) {
        evidence.forEach(e -> state.markCorroborated(e.eventId(), CORROBORATION_MEMORY));
    }

    public List<AlertEvent> sweepPending() {
        List<AlertEvent> alerts = new ArrayList<>();
        sweepPending(alerts::add);
        return alerts;
    }

    public void sweepPending(AlertSink sink) {
        // Durante una puesta al día no se clasifican falsos positivos: la evidencia puede venir detrás en la cola.
        if (clock.millis() < localHoldUntil || consumersFailing() || state.isBacklogged()) {
            log.debug("Puesta al dia en curso: se pospone la clasificacion de falsos positivos");
            return;
        }
        for (RuleDefinition rule : properties.getRules()) {
            if (!rule.isFalsePositiveCheck()) {
                continue;
            }
            List<NormalizedEvent> claimed = state.claimExpiredPending(rule.getId(), clock.instant(), 100);
            for (int i = 0; i < claimed.size(); i++) {
                NormalizedEvent anchor = claimed.get(i);
                try {
                    classifyExpired(rule, anchor, sink);
                } catch (RuntimeException e) {
                    Instant retry = clock.instant().plusSeconds(5);
                    for (NormalizedEvent pending : claimed.subList(i, claimed.size())) {
                        state.addPending(rule.getId(), pending, retry);
                    }
                    throw e;
                }
            }
        }
    }

    private void classifyExpired(RuleDefinition rule, NormalizedEvent anchor, AlertSink sink) {
        if (state.isCorroborated(anchor.eventId()) || isFlaggedPanic(anchor)) {
            return;
        }
        RuleMatch last = evaluate(rule, anchor);
        if (last.reachedThreshold()) {
            Instant latest = last.evidence().stream().map(Evidence::eventTime)
                    .max(Instant::compareTo).orElse(anchor.eventTime());
            openOrEscalate(last, latest, sink);
            return;
        }
        sink.publish(falsePositive(rule, last));
        state.markCorroborated(anchor.eventId(), CORROBORATION_MEMORY);
    }

    private AlertEvent falsePositive(RuleDefinition rule, RuleMatch match) {
        NormalizedEvent anchor = match.anchor();
        String what = match.evidence().size() > 1 ? "con corroboracion insuficiente" : "sin eventos complementarios";
        String description = String.format(Locale.ROOT,
                "%s %s en %s durante %d s. Puntaje %.2f < umbral %.2f. Evidencia: %s.",
                rule.getAnchor().getLabel(), what, anchor.zoneName(), properties.getFalsePositiveTimeout().toSeconds(),
                match.score(), match.threshold(), String.join("; ", match.reasons()));
        log.info("POSIBLE FALSO POSITIVO {} en {} (regla {})", anchor.summary(), anchor.zoneName(), rule.getId());
        return new AlertEvent(deterministicId("fp:" + rule.getId() + ":" + anchor.eventId()), 1,
                rule.getId() + "/sin-corroborar", FALSE_POSITIVE, Severity.LOW,
                AlertStatus.AUTO_CLASSIFIED, anchor.zoneId(), anchor.zoneName(), anchor.latitude(), anchor.longitude(),
                match.score(), "Posible falso positivo: " + rule.getTitle().toLowerCase(Locale.ROOT) + " sin corroborar",
                description, clock.instant(), anchor.eventTime(), anchor.eventTime(), match.evidence(), null, null, null);
    }

    private void checkDeviceAbuse(NormalizedEvent panic, AlertSink sink) {
        CorrelationProperties.DeviceAbuse config = properties.getDeviceAbuse();
        long count = state.incrementDevicePanics(panic.sensorId(), config.getWindow());
        if (count <= config.getMaxPanics() || state.isDeviceFlagged(panic.sensorId())) {
            return;
        }
        String device = panic.sensorId().startsWith("APP-") ? "Un dispositivo de la app" : "El boton " + panic.sensorId();
        String description = String.format(Locale.ROOT,
                "%s presiono el boton de panico %d veces en %d min. Sus eventos se ignoraran "
                        + "durante %d min para evitar falsas alarmas; revisar si es vandalismo, falla o abuso.",
                device, count, config.getWindow().toMinutes(), config.getFlagDuration().toMinutes());
        log.info("DISPOSITIVO SOSPECHOSO {} en {}", panic.sensorId(), panic.zoneName());
        sink.publish(new AlertEvent(deterministicId("abuse:" + panic.eventId()), 1, "R0-ABUSO-DISPOSITIVO", SUSPICIOUS_DEVICE, Severity.MEDIUM,
                AlertStatus.NEW, panic.zoneId(), panic.zoneName(), panic.latitude(), panic.longitude(), 1.0,
                "Dispositivo con uso anomalo del boton de panico", description, clock.instant(), panic.eventTime(),
                panic.eventTime(), List.of(Evidence.of(panic, 1.0)), null, null, null));
        state.flagDevice(panic.sensorId(), config.getFlagDuration());
    }

    private boolean isFlaggedPanic(NormalizedEvent event) {
        return event.sensorType() == SensorType.PANIC && state.isDeviceFlagged(event.sensorId());
    }

    private void checkSuspectVehicle(NormalizedEvent lpr, AlertSink sink) {
        Optional<SuspectState> found = state.suspect(lpr.plateHash());
        if (found.isEmpty()) {
            return;
        }
        SuspectState suspect = found.get();
        if (lpr.zoneId() == suspect.originZoneId() || state.suspectSeenInZone(suspect.alertId(), lpr.zoneId())) {
            return;
        }
        Duration elapsed = Duration.between(suspect.registeredAt(), clock.instant());
        Severity severity = lpr.anomalous() ? Severity.CRITICAL : Severity.HIGH;
        String description = String.format(Locale.ROOT,
                "Vehiculo placa %s vinculado al incidente %s de Zona %d detectado en %s a %.0f km/h, %d min %d s despues.",
                lpr.plateMasked(), suspect.alertType(), suspect.originZoneId(), lpr.zoneName(),
                lpr.value() == null ? 0 : lpr.value(), elapsed.toMinutes(), elapsed.toSecondsPart());
        log.info("VEHICULO EN FUGA {} en {}", lpr.plateMasked(), lpr.zoneName());
        sink.publish(new AlertEvent(deterministicId("fuga:" + suspect.alertId() + ":" + lpr.zoneId()), 1,
                "R8-SEGUIMIENTO-PLACA", VEHICLE_ESCAPING, severity,
                AlertStatus.NEW, lpr.zoneId(), lpr.zoneName(), lpr.latitude(), lpr.longitude(), 0.9,
                "Vehiculo sospechoso en fuga", description, clock.instant(), lpr.eventTime(), lpr.eventTime(),
                List.of(Evidence.of(lpr, 0.9)), lpr.plateMasked(), lpr.plateHash(), suspect.alertId()));
        state.firstSuspectSightingInZone(suspect.alertId(), lpr.zoneId(), properties.getSuspectTracking());
    }
}
