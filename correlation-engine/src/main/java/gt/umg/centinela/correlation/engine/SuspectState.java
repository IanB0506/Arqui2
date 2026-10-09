package gt.umg.centinela.correlation.engine;

import java.time.Instant;
import java.util.UUID;

/** Vehiculo sospechoso asociado a un incidente (se sigue entre zonas por su seudonimo de placa). */
public record SuspectState(UUID alertId, String alertType, int originZoneId, Instant registeredAt, String plateMasked) {
}
