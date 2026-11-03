package gt.umg.centinela.common.model;

import java.time.Instant;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

public record Evidence(
        UUID eventId,
        SensorType sensorType,
        String subtype,
        String sensorId,
        Instant eventTime,
        double weight,
        String summary,
        String detail) {
    public static Evidence of(NormalizedEvent event, double weight) {
        return new Evidence(event.eventId(), event.sensorType(), event.subtype(), event.sensorId(),
                event.eventTime(), weight, event.summary(), detailOf(event));
    }

    static String detailOf(NormalizedEvent event) {
        Map<String, String> a = event.attributes() == null ? Map.of() : event.attributes();
        return switch (event.sensorType()) {
            case CITIZEN_REPORT -> a.containsKey(NormalizedEvent.ATTR_MESSAGE)
                    ? "\"" + a.get(NormalizedEvent.ATTR_MESSAGE) + "\"" : null;
            case LPR -> join(a.get(NormalizedEvent.ATTR_VEHICLE_MODEL), a.get(NormalizedEvent.ATTR_VEHICLE_COLOR));
            case SPEED -> a.containsKey(NormalizedEvent.ATTR_DIRECTION)
                    ? "sentido " + a.get(NormalizedEvent.ATTR_DIRECTION).toLowerCase(Locale.ROOT)
                    + (a.containsKey(NormalizedEvent.ATTR_SPEED_LIMIT) ? ", limite " + a.get(NormalizedEvent.ATTR_SPEED_LIMIT) + " km/h" : "")
                    : null;
            case PANIC -> "APP".equals(event.origin())
                    ? "desde la app" + (a.containsKey(NormalizedEvent.ATTR_PLATFORM) ? " (" + a.get(NormalizedEvent.ATTR_PLATFORM) + ")" : "")
                    : "boton fisico";
            case ACOUSTIC -> String.format(Locale.ROOT, "probabilidad %.2f", event.confidence());
        };
    }

    private static String join(String first, String second) {
        if (first == null) {
            return second;
        }
        return second == null ? first : first + " " + second.toLowerCase(Locale.ROOT);
    }
}
