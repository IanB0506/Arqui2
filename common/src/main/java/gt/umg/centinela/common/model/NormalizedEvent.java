package gt.umg.centinela.common.model;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Evento normalizado: formato unico al que la ingesta convierte los cinco tipos de sensores.
 *
 * <p>Gracias a este formato comun, el motor de correlacion y la persistencia no necesitan
 * conocer el esquema particular de cada sensor (datos heterogeneos -> modelo homogeneo).
 *
 * @param eventId     identificador unico del evento (se conserva desde el sensor: trazabilidad)
 * @param sensorType  tipo de sensor de origen
 * @param sensorId    identificador del sensor o dispositivo
 * @param zoneId      zona resuelta (1..12)
 * @param zoneName    nombre legible de la zona ("Zona 4")
 * @param latitude    latitud (generalizada a ~110 m si proviene de un ciudadano)
 * @param longitude   longitud
 * @param eventTime   momento en que ocurrio el evento (reloj del sensor)
 * @param ingestedAt  momento en que la ingesta lo proceso
 * @param subtype     clasificacion especifica: ROBO, DISPARO, ACCIDENTE, EXCESO_VELOCIDAD...
 * @param value       valor numerico principal: km/h para velocidad/LPR, dB para acusticos
 * @param confidence  probabilidad 0..1 de que el evento sea real (acusticos la reportan)
 * @param anomalous   true si el evento por si mismo es anomalo (exceso de velocidad, etc.)
 * @param plateHash   seudonimo HMAC de la placa (nunca se guarda la placa en claro)
 * @param plateMasked placa enmascarada para mostrar a operadores ("XYZ***")
 * @param sourceHash  seudonimo del ciudadano que origino el evento (si aplica)
 * @param origin      origen: SENSOR, APP, PUNTO_FISICO, USUARIO
 * @param attributes  atributos adicionales no estructurados
 */
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

    /** Atributo que marca un evento reenviado desde un spool: llego tarde por una caida. */
    public static final String ATTR_SPOOLED = "reenviado_spool";
    /** Texto libre de un reporte ciudadano (dato no estructurado, con las placas enmascaradas). */
    public static final String ATTR_MESSAGE = "mensaje";
    public static final String ATTR_VEHICLE_MODEL = "modelo";
    public static final String ATTR_VEHICLE_COLOR = "color";
    public static final String ATTR_DIRECTION = "direccion";
    public static final String ATTR_SPEED_LIMIT = "limite";
    public static final String ATTR_PLATFORM = "plataforma";

    /** true si el evento espero en un spool mientras Kafka no estaba disponible. */
    public boolean replayedFromSpool() {
        return attributes != null && "true".equals(attributes.get(ATTR_SPOOLED));
    }

    /** Texto corto y legible del evento, usado como evidencia en las alertas. */
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
