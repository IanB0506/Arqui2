package gt.umg.centinela.correlation.engine;

import gt.umg.centinela.common.model.AlertEvent;

/**
 * Destino de las alertas del motor. Debe publicar de forma sincronica y lanzar una excepcion si no
 * pudo: el motor solo guarda el estado del incidente DESPUES de publicar, asi un fallo de Kafka no
 * deja un incidente "ya avisado" cuya alerta nunca salio (el reintento la vuelve a emitir).
 */
@FunctionalInterface
public interface AlertSink {

    void publish(AlertEvent alert);
}
