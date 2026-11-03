package gt.umg.centinela.simulator.generator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

@Component
// Mientras corre un escenario, el tráfico de fondo de esas zonas solo emite lecturas normales.
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
