package gt.umg.centinela.ingestion.pipeline;

import gt.umg.centinela.common.geo.Zone;
import gt.umg.centinela.common.geo.ZoneCatalog;
import gt.umg.centinela.common.json.InvalidEventException;
import gt.umg.centinela.common.json.Json;
import gt.umg.centinela.common.model.NormalizedEvent;
import gt.umg.centinela.common.model.SensorType;
import gt.umg.centinela.common.model.raw.AcousticSensorEvent;
import gt.umg.centinela.common.model.raw.CitizenEventType;
import gt.umg.centinela.common.model.raw.CitizenReportEvent;
import gt.umg.centinela.common.model.raw.LprCameraEvent;
import gt.umg.centinela.common.model.raw.PanicButtonEvent;
import gt.umg.centinela.common.model.raw.SoundType;
import gt.umg.centinela.common.model.raw.SpeedSensorEvent;
import gt.umg.centinela.common.model.raw.ZoneRef;
import gt.umg.centinela.common.privacy.PlatePseudonymizer;
import gt.umg.centinela.ingestion.config.IngestionProperties;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Convierte cada contrato crudo al {@link NormalizedEvent} comun y aplica las reglas de privacidad
 * del ADR-007:
 * <ul>
 *   <li>Placas: se reemplazan por un seudonimo HMAC y una version enmascarada ("XYZ***").</li>
 *   <li>Ciudadanos: el id se seudonimiza y las coordenadas se generalizan a ~110 m.</li>
 *   <li>Dispositivos de la app: su id tambien se seudonimiza; los postes fisicos conservan su id.</li>
 * </ul>
 * Es una clase sin dependencias de infraestructura para poder probarla con pruebas unitarias.
 */
public class EventNormalizer {

    /** Atributo que marca un evento reenviado desde un spool (llego tarde por una caida). */
    public static final String ATTR_SPOOLED = NormalizedEvent.ATTR_SPOOLED;
    /** Segundos entre que ocurrio el evento y que llego a la ingesta. */
    public static final String ATTR_ORIGIN_DELAY = "retraso_origen_s";

    /** Probabilidad minima para considerar anomalo un sonido critico. */
    static final double ACOUSTIC_ANOMALY_THRESHOLD = 0.5;

    private final IngestionProperties properties;
    private final PlatePseudonymizer pseudonymizer;
    private final Validator validator;
    private final Clock clock;

    public EventNormalizer(IngestionProperties properties, PlatePseudonymizer pseudonymizer, Validator validator, Clock clock) {
        this.properties = properties;
        this.pseudonymizer = pseudonymizer;
        this.validator = validator;
        this.clock = clock;
    }

    public NormalizedEvent normalize(SensorType type, String json) {
        return normalize(type, json, null, false);
    }

    /**
     * @param reference hora en que el evento entro al sistema (header centinela-first-attempt o el
     *                  timestamp del mensaje en Kafka). La edad maxima se mide contra ella: lo que espero
     *                  en Kafka o en un spool mientras un nodo estaba caido no cuenta como antiguedad.
     * @param spooled   true si el evento se reenvio desde un spool: queda marcado en los atributos para
     *                  que el alert-service sepa que llego tarde por una caida
     */
    public NormalizedEvent normalize(SensorType type, String json, Instant reference, boolean spooled) {
        NormalizedEvent event = switch (type) {
            case PANIC -> panic(parse(json, PanicButtonEvent.class));
            case LPR -> lpr(parse(json, LprCameraEvent.class));
            case SPEED -> speed(parse(json, SpeedSensorEvent.class));
            case ACOUSTIC -> acoustic(parse(json, AcousticSensorEvent.class));
            case CITIZEN_REPORT -> citizen(parse(json, CitizenReportEvent.class));
        };
        checkAge(event.eventTime(), reference);
        if (!spooled) {
            return event;
        }
        Map<String, String> attributes = new LinkedHashMap<>(event.attributes());
        attributes.put(ATTR_SPOOLED, "true");
        attributes.put(ATTR_ORIGIN_DELAY, String.valueOf(Math.max(0, Duration.between(event.eventTime(), now()).toSeconds())));
        return new NormalizedEvent(event.eventId(), event.sensorType(), event.sensorId(), event.zoneId(), event.zoneName(),
                event.latitude(), event.longitude(), event.eventTime(), event.ingestedAt(), event.subtype(), event.value(),
                event.confidence(), event.anomalous(), event.plateHash(), event.plateMasked(), event.sourceHash(),
                event.origin(), attributes);
    }

    /** Deserializa y valida el contrato; cualquier falla es permanente (InvalidEventException -> DLQ). */
    public <T> T parse(String json, Class<T> type) {
        T event = Json.read(json, type);
        if (event == null) {
            throw new InvalidEventException("Mensaje vacio");
        }
        Set<ConstraintViolation<T>> violations = validator.validate(event);
        if (!violations.isEmpty()) {
            String detail = violations.stream()
                    .map(v -> v.getPropertyPath() + " " + v.getMessage())
                    .sorted()
                    .collect(Collectors.joining("; "));
            throw new InvalidEventException("Contrato invalido de " + type.getSimpleName() + ": " + detail);
        }
        return event;
    }

    NormalizedEvent panic(PanicButtonEvent e) {
        checkFuture(e.timestamp());
        Zone zone = resolveZone(e.zone());
        boolean fromApp = e.userContext() != null || e.deviceId().startsWith("APP-");
        double[] point = fromApp ? generalize(pointOf(e.zone(), zone)) : pointOf(e.zone(), zone);
        Map<String, String> attributes = new LinkedHashMap<>();
        if (e.userContext() != null) {
            put(attributes, NormalizedEvent.ATTR_PLATFORM, e.userContext().platform());
            put(attributes, "version_app", e.userContext().appVersion());
        }
        String sensorId = fromApp ? "APP-" + pseudonymizer.pseudonymizeId(e.deviceId()).substring(0, 12) : e.deviceId();
        String user = e.userContext() != null && e.userContext().userId() != null ? e.userContext().userId() : e.deviceId();
        return new NormalizedEvent(e.eventId(), SensorType.PANIC, sensorId, zone.id(), zone.name(), point[0], point[1],
                e.timestamp(), now(), e.alertType().name(), null, 1.0, true, null, null,
                pseudonymizer.pseudonymizeId(user), fromApp ? "APP" : "PUNTO_FISICO", attributes);
    }

    NormalizedEvent lpr(LprCameraEvent e) {
        checkFuture(e.timestamp());
        Zone zone = e.location().zoneName() != null
                ? ZoneCatalog.byName(e.location().zoneName()).orElseThrow(() -> unknownZone(e.location().zoneName()))
                : nearestOrFail(e.location().latitude(), e.location().longitude());
        double lat = e.location().latitude() != null ? e.location().latitude() : zone.latitude();
        double lon = e.location().longitude() != null ? e.location().longitude() : zone.longitude();
        boolean speeding = e.estimatedSpeed() > threshold(properties.defaultSpeedLimit());
        Map<String, String> attributes = new LinkedHashMap<>();
        put(attributes, NormalizedEvent.ATTR_VEHICLE_MODEL, e.vehicleModel());
        put(attributes, NormalizedEvent.ATTR_VEHICLE_COLOR, e.vehicleColor());
        return new NormalizedEvent(e.eventId(), SensorType.LPR, e.location().sensorId(), zone.id(), zone.name(), lat, lon,
                e.timestamp(), now(), speeding ? "EXCESO_VELOCIDAD" : "LECTURA_PLACA", e.estimatedSpeed(), 1.0, speeding,
                pseudonymizer.pseudonymizePlate(e.plate()), PlatePseudonymizer.maskPlate(e.plate()), null, "SENSOR",
                attributes);
    }

    NormalizedEvent speed(SpeedSensorEvent e) {
        checkFuture(e.timestamp());
        Zone zone = ZoneCatalog.byName(e.zoneName()).orElseThrow(() -> unknownZone(e.zoneName()));
        int limit = e.speedLimit() != null ? e.speedLimit() : properties.defaultSpeedLimit();
        boolean speeding = e.speed() > threshold(limit);
        boolean wrongWay = Boolean.TRUE.equals(e.wrongWay());
        String subtype = wrongWay ? "CONTRAVIA" : speeding ? "EXCESO_VELOCIDAD" : "NORMAL";
        Map<String, String> attributes = new LinkedHashMap<>();
        attributes.put(NormalizedEvent.ATTR_DIRECTION, e.direction().name());
        attributes.put(NormalizedEvent.ATTR_SPEED_LIMIT, String.valueOf(limit));
        return new NormalizedEvent(e.eventId(), SensorType.SPEED, e.sensorId(), zone.id(), zone.name(),
                zone.latitude(), zone.longitude(), e.timestamp(), now(), subtype, e.speed(), 1.0,
                speeding || wrongWay, null, null, null, "SENSOR", attributes);
    }

    NormalizedEvent acoustic(AcousticSensorEvent e) {
        checkFuture(e.timestamp());
        Zone zone = ZoneCatalog.byName(e.zoneName()).orElseThrow(() -> unknownZone(e.zoneName()));
        boolean anomalous = e.soundType() != SoundType.RUIDO_AMBIENTAL
                && e.criticalProbability() >= ACOUSTIC_ANOMALY_THRESHOLD;
        return new NormalizedEvent(e.eventId(), SensorType.ACOUSTIC, e.sensorId(), zone.id(), zone.name(),
                zone.latitude(), zone.longitude(), e.timestamp(), now(), e.soundType().name(), e.decibels(),
                e.criticalProbability(), anomalous, null, null, null, "SENSOR", Map.of());
    }

    NormalizedEvent citizen(CitizenReportEvent e) {
        checkFuture(e.timestamp());
        Zone zone = resolveZone(e.location());
        double[] point = generalize(pointOf(e.location(), zone));
        Map<String, String> attributes = new LinkedHashMap<>();
        if (e.message() != null) {
            // dato no estructurado: se conserva para el operador, pero sin placas en claro
            String message = PlatePseudonymizer.maskPlatesInText(e.message());
            attributes.put(NormalizedEvent.ATTR_MESSAGE, message.length() > 200 ? message.substring(0, 200) : message);
        }
        return new NormalizedEvent(e.eventId(), SensorType.CITIZEN_REPORT, "REPORTE-" + e.origin().name(), zone.id(),
                zone.name(), point[0], point[1], e.timestamp(), now(), e.eventType().name(), null, 1.0,
                e.eventType() != CitizenEventType.OTRO, null, null, pseudonymizer.pseudonymizeId(e.reporterId()),
                e.origin().name(), attributes);
    }

    // ------------------------------------------------------------------ utilidades

    private double threshold(int limit) {
        return limit * (1 + properties.speedTolerance());
    }

    private void checkFuture(Instant timestamp) {
        if (timestamp.isAfter(now().plus(properties.maxClockSkew()))) {
            throw new InvalidEventException("Timestamp en el futuro (reloj del sensor desincronizado): " + timestamp);
        }
    }

    private void checkAge(Instant timestamp, Instant reference) {
        Instant now = now();
        // La edad se mide contra el momento en que el evento entro al sistema; si ese reloj viene
        // adelantado (o no hay referencia) se usa la hora de proceso.
        Instant base = reference != null && reference.isBefore(now) ? reference : now;
        if (!properties.allowHistorical() && timestamp.isBefore(base.minus(properties.maxEventAge()))) {
            throw new InvalidEventException("Evento demasiado antiguo: " + timestamp + " (entro al sistema " + base + ")");
        }
    }

    private Zone resolveZone(ZoneRef ref) {
        if (ref.name() != null && !ref.name().isBlank()) {
            return ZoneCatalog.byName(ref.name()).orElseThrow(() -> unknownZone(ref.name()));
        }
        return nearestOrFail(ref.latitude(), ref.longitude());
    }

    private Zone nearestOrFail(Double lat, Double lon) {
        if (lat == null || lon == null) {
            throw new InvalidEventException("El evento no trae zona ni coordenadas");
        }
        Zone zone = ZoneCatalog.nearest(lat, lon);
        if (zone.distanceKm(lat, lon) > 5) {
            throw new InvalidEventException("Coordenadas fuera del area monitoreada: " + lat + "," + lon);
        }
        return zone;
    }

    private static double[] pointOf(ZoneRef ref, Zone zone) {
        return ref.hasCoordinates()
                ? new double[]{ref.latitude(), ref.longitude()}
                : new double[]{zone.latitude(), zone.longitude()};
    }

    private double[] generalize(double[] point) {
        double factor = Math.pow(10, properties.coordinateDecimals());
        return new double[]{Math.round(point[0] * factor) / factor, Math.round(point[1] * factor) / factor};
    }

    private static InvalidEventException unknownZone(String name) {
        return new InvalidEventException("Zona desconocida: " + name);
    }

    private static void put(Map<String, String> map, String key, String value) {
        if (value != null) {
            map.put(key, value);
        }
    }

    private Instant now() {
        return clock.instant();
    }
}
