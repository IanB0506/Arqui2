package gt.umg.centinela.alerts.live;

import gt.umg.centinela.alerts.persistence.AlertView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Envia alertas en tiempo real a las consolas abiertas mediante Server-Sent Events (SSE):
 * una conexion HTTP que el servidor mantiene abierta y por la que "empuja" cada alerta nueva.
 */
@Component
public class AlertBroadcaster {

    private static final Logger log = LoggerFactory.getLogger(AlertBroadcaster.class);
    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    public SseEmitter subscribe() {
        SseEmitter emitter = new SseEmitter(30 * 60 * 1000L);
        emitters.add(emitter);
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));
        emitter.onError(e -> emitters.remove(emitter));
        return emitter;
    }

    public void broadcast(AlertView alert) {
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event().name("alert").data(alert));
            } catch (IOException | IllegalStateException e) {
                emitters.remove(emitter);
                log.debug("Consola desconectada: {}", e.getMessage());
            }
        }
    }

    public int connected() {
        return emitters.size();
    }
}
