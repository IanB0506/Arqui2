package gt.umg.centinela.correlation.engine;

import gt.umg.centinela.common.model.NormalizedEvent;
import gt.umg.centinela.common.model.SensorType;

import java.time.Instant;
import java.util.List;

/** Ventanas deslizantes de eventos recientes por zona y tipo de sensor. */
public interface WindowStore {

    void add(NormalizedEvent event);

    List<NormalizedEvent> range(int zoneId, SensorType type, Instant from, Instant to);
}
