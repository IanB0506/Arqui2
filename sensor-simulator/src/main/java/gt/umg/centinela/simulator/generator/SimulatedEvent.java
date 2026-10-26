package gt.umg.centinela.simulator.generator;

import gt.umg.centinela.common.model.SensorType;

import java.util.UUID;

/**
 * Evento listo para publicarse.
 *
 * @param type    tipo de sensor (define el topic destino)
 * @param key     llave del mensaje en Kafka (id del sensor o dispositivo)
 * @param eventId id unico del evento (se conserva en todo el flujo: trazabilidad)
 * @param payload objeto con el contrato crudo del sensor
 */
public record SimulatedEvent(SensorType type, String key, UUID eventId, Object payload) {
}
