package gt.umg.centinela.simulator.generator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Zonas donde corre un escenario. Mientras dure, el trafico de fondo solo emite lecturas normales en
 * ellas (sin panicos, reportes, excesos de velocidad ni sonidos criticos): asi el ruido aleatorio no
 * corrobora ni contamina el escenario y su resultado es reproducible, por ejemplo la fila 2 de la
 * tabla 2.4 (un panico solo en la Zona 10 debe terminar como posible falso positivo).
 */
@Component
public class QuietZones {

    private final Map<Integer, Instant> until = new ConcurrentHashMap<>();
    private final Clock clock;

    @Autowired
    public QuietZones() {
        this(Clock.systemUTC());
    }

    QuietZones(Clock clock) {
        this.clock = clock;
    }

    /** Aisla las zonas hasta {@code end} (si ya estaban aisladas por mas tiempo, se conserva lo mayor). */
    public void reserve(Collection<Integer> zones, Instant end) {
        for (Integer zone : zones) {
            until.merge(zone, end, (current, requested) -> current.isAfter(requested) ? current : requested);
        }
    }

    public boolean isQuiet(int zone) {
        Instant end = until.get(zone);
        if (end == null) {
            return false;
        }
        if (clock.instant().isBefore(end)) {
            return true;
        }
        until.remove(zone, end);
        return false;
    }

    /** Zonas aisladas en este momento y hasta cuando. */
    public Map<Integer, Instant> active() {
        Map<Integer, Instant> active = new TreeMap<>();
        until.forEach((zone, end) -> {
            if (clock.instant().isBefore(end)) {
                active.put(zone, end);
            }
        });
        return active;
    }
}
