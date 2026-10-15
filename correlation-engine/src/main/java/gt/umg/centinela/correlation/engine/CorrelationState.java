package gt.umg.centinela.correlation.engine;

import gt.umg.centinela.common.model.NormalizedEvent;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Estado compartido del motor (en Redis en produccion, en memoria en las pruebas).
 * Todas las operaciones de "reclamo" son atomicas para que varias instancias del motor en
 * distintas maquinas no emitan la misma alerta dos veces.
 */
public interface CorrelationState {

    Optional<IncidentState> incident(String key);

    void saveIncident(String key, IncidentState state, Duration ttl);

    void markCorroborated(UUID eventId, Duration ttl);

    boolean isCorroborated(UUID eventId);

    void addPending(String ruleId, NormalizedEvent anchor, Instant deadline);

    /** Devuelve y elimina (atomicamente) las anclas cuyo plazo de corroboracion ya vencio. */
    List<NormalizedEvent> claimExpiredPending(String ruleId, Instant now, int limit);

    /** Incrementa el contador de panicos de un dispositivo y devuelve el total en la ventana. */
    long incrementDevicePanics(String sensorId, Duration window);

    /** Marca un dispositivo como sospechoso. Devuelve true solo la primera vez (para alertar una vez). */
    boolean flagDevice(String sensorId, Duration ttl);

    boolean isDeviceFlagged(String sensorId);

    void registerSuspect(String plateHash, SuspectState suspect, Duration ttl);

    Optional<SuspectState> suspect(String plateHash);

    /**
     * true si es la primera vez que el vehiculo sospechoso de ESTE incidente se ve en esa zona. Se
     * registra por incidente (no por placa): si la misma placa participa en un incidente nuevo, su
     * recorrido se vuelve a seguir desde cero.
     */
    boolean firstSuspectSightingInZone(UUID alertId, int zoneId, Duration ttl);

    /** true si el vehiculo de ese incidente ya se aviso en esa zona (consulta sin registrar). */
    boolean suspectSeenInZone(UUID alertId, int zoneId);

    /** Avisa a todas las instancias que hay atraso en proceso (puesta al dia tras una caida). */
    void markBacklog(Duration ttl);

    boolean isBacklogged();

    boolean isWatchlisted(String plateHash);

    Map<String, Double> thresholdAdjustments();
}
