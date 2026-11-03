package gt.umg.centinela.correlation.engine;

import gt.umg.centinela.common.model.AlertEvent;

@FunctionalInterface
public interface AlertSink {
    void publish(AlertEvent alert);
}
