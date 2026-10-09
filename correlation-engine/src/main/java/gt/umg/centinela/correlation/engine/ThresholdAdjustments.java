package gt.umg.centinela.correlation.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Cache local de los ajustes adaptativos de umbral. El alert-service los calcula cada hora a partir
 * de las validaciones de los operadores: si en una zona una regla genera muchos falsos positivos,
 * su umbral sube (se exige mas evidencia). Se refresca cada 30 s para no consultar Redis por evento.
 */
@Component
public class ThresholdAdjustments implements Supplier<Map<String, Double>> {

    private static final Logger log = LoggerFactory.getLogger(ThresholdAdjustments.class);

    private final CorrelationState state;
    private volatile Map<String, Double> current = Map.of();

    public ThresholdAdjustments(CorrelationState state) {
        this.state = state;
    }

    @Scheduled(fixedDelay = 30_000, initialDelay = 1_000)
    public void refresh() {
        try {
            Map<String, Double> latest = state.thresholdAdjustments();
            if (!latest.equals(current)) {
                log.info("Ajustes de umbral actualizados: {}", latest);
            }
            current = Map.copyOf(latest);
        } catch (RuntimeException e) {
            log.warn("No se pudieron leer los ajustes de umbral: {}", e.getMessage());
        }
    }

    @Override
    public Map<String, Double> get() {
        return current;
    }
}
