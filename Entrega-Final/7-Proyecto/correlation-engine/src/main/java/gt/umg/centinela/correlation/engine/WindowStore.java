package gt.umg.centinela.correlation.engine;

import gt.umg.centinela.common.model.NormalizedEvent;
import gt.umg.centinela.common.model.SensorType;

import java.time.Instant;
import java.util.List;

public interface WindowStore {
    void add(NormalizedEvent event);

    List<NormalizedEvent> range(int zoneId, SensorType type, Instant from, Instant to);
}
