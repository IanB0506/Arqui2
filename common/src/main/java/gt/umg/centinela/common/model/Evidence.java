package gt.umg.centinela.common.model;

import java.time.Instant;
import java.util.UUID;

/** Evento que forma parte de la evidencia de una alerta (permite rastrear la alerta hasta su origen). */
public record Evidence(
        UUID eventId,
        SensorType sensorType,
        String subtype,
        String sensorId,
        Instant eventTime,
        double weight,
        String summary) {

    public static Evidence of(NormalizedEvent event, double weight) {
        return new Evidence(event.eventId(), event.sensorType(), event.subtype(), event.sensorId(),
                event.eventTime(), weight, event.summary());
    }
}
