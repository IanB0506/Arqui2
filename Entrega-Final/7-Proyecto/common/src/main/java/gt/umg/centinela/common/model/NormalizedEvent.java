package gt.umg.centinela.common.model;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record NormalizedEvent(
        UUID eventId,
        SensorType sensorType,
        String sensorId,
        int zoneId,
        String zoneName,
        double latitude,
        double longitude,
        Instant eventTime,
        Instant ingestedAt,
        String subtype,
        Double value,
        double confidence,
        boolean anomalous,
        String plateHash,
        String plateMasked,
        String sourceHash,
        String origin,
        Map<String, String> attributes) {
    public static final String ATTR_SPOOLED = "reenviado_spool";
    public static final String ATTR_MESSAGE = "mensaje";
    public static final String ATTR_VEHICLE_MODEL = "modelo";
    public static final String ATTR_VEHICLE_COLOR = "color";
    public static final String ATTR_DIRECTION = "direccion";
    public static final String ATTR_SPEED_LIMIT = "limite";
    public static final String ATTR_PLATFORM = "plataforma";

    public boolean replayedFromSpool() {
        return attributes != null && "true".equals(attributes.get(ATTR_SPOOLED));
    }

    public String summary() {
        StringBuilder sb = new StringBuilder(sensorType.label()).append(' ').append(subtype);
        if (value != null) {
            sb.append(' ').append(String.format(java.util.Locale.ROOT, "%.1f", value));
            sb.append(sensorType == SensorType.ACOUSTIC ? " dB" : " km/h");
        }
        if (plateMasked != null) {
            sb.append(" placa ").append(plateMasked);
        }
        return sb.toString();
    }
}
