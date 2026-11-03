package gt.umg.centinela.correlation.engine;

import java.time.Instant;
import java.util.UUID;

public record SuspectState(UUID alertId, String alertType, int originZoneId, Instant registeredAt, String plateMasked) {
}
