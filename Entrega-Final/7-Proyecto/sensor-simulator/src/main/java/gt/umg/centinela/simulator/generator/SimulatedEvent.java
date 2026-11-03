package gt.umg.centinela.simulator.generator;

import gt.umg.centinela.common.model.SensorType;

import java.util.UUID;

public record SimulatedEvent(SensorType type, String key, UUID eventId, Object payload) {
}
