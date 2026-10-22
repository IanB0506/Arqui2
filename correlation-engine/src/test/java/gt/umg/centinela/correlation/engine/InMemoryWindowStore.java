package gt.umg.centinela.correlation.engine;

import gt.umg.centinela.common.model.NormalizedEvent;
import gt.umg.centinela.common.model.SensorType;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/** Ventanas en memoria para pruebas. */
class InMemoryWindowStore implements WindowStore {

    private final List<NormalizedEvent> events = new ArrayList<>();

    @Override
    public void add(NormalizedEvent event) {
        events.removeIf(e -> e.eventId().equals(event.eventId()));
        events.add(event);
    }

    @Override
    public List<NormalizedEvent> range(int zoneId, SensorType type, Instant from, Instant to) {
        return events.stream()
                .filter(e -> e.zoneId() == zoneId && e.sensorType() == type)
                .filter(e -> !e.eventTime().isBefore(from) && !e.eventTime().isAfter(to))
                .toList();
    }
}
