package gt.umg.centinela.common.scheduling;

import gt.umg.centinela.common.kafka.FailureClassifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ErrorHandler;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manejador de errores de las tareas periodicas (ADR-006). Con el nodo A caido, cada tarea que toca
 * Redis, la base o Kafka falla en cada ronda, y el manejador por defecto de Spring escribe la traza
 * completa cada vez (cientos de lineas por minuto que esconden lo importante).
 *
 * <p>Aqui, una falla transitoria (dependencia no disponible) se registra en UNA linea, como mucho una
 * vez por minuto y por tarea, con cuantas veces se repitio; la tarea vuelve a correr en su siguiente
 * ronda. Cualquier otro error es un defecto y conserva su traza completa.
 */
public final class ScheduledTaskErrorHandler implements ErrorHandler {

    private static final Logger log = LoggerFactory.getLogger(ScheduledTaskErrorHandler.class);
    private static final String OWN_PACKAGE = "gt.umg.centinela.";

    private final Clock clock;
    private final Duration quietPeriod;
    private final Map<String, Window> windows = new ConcurrentHashMap<>();

    public ScheduledTaskErrorHandler() {
        this(Clock.systemUTC(), Duration.ofMinutes(1));
    }

    ScheduledTaskErrorHandler(Clock clock, Duration quietPeriod) {
        this.clock = clock;
        this.quietPeriod = quietPeriod;
    }

    @Override
    public void handleError(Throwable error) {
        String task = taskName(error);
        if (!FailureClassifier.isTransient(error)) {
            log.error("Error inesperado en la tarea periodica {}", task, error);
            return;
        }
        long now = clock.millis();
        Window window = windows.computeIfAbsent(task, k -> new Window());
        int repeated = window.suppressed.incrementAndGet() - 1;
        synchronized (window) {
            if (now - window.lastLogged < quietPeriod.toMillis()) {
                return;
            }
            window.lastLogged = now;
            window.suppressed.set(0);
        }
        Throwable root = FailureClassifier.rootCause(error);
        log.warn("Tarea periodica {} pospuesta: dependencia no disponible ({}: {}){}. Se reintenta en su siguiente ronda.",
                task, root.getClass().getSimpleName(), root.getMessage(),
                repeated > 0 ? " [se repitio " + repeated + " veces en el ultimo minuto]" : "");
    }

    /**
     * "WatchlistSyncJob.checkSentinel": el primer metodo propio en la traza (sin proxies de Spring). Si la
     * falla ocurrio al tomar el bloqueo de ShedLock (antes de entrar a la tarea), lo dice.
     */
    static String taskName(Throwable error) {
        boolean shedLock = false;
        for (Throwable t = error; t != null; t = t.getCause() == t ? null : t.getCause()) {
            for (StackTraceElement frame : t.getStackTrace()) {
                String cls = frame.getClassName();
                if (cls.startsWith(OWN_PACKAGE) && !cls.contains("$$") && !cls.startsWith(OWN_PACKAGE + "common.")) {
                    return cls.substring(cls.lastIndexOf('.') + 1) + "." + frame.getMethodName();
                }
                shedLock |= cls.startsWith("net.javacrumbs.shedlock.");
            }
        }
        return shedLock ? "(bloqueo de ShedLock en la base)" : "(desconocida)";
    }

    int pendingRepeats(String task) {
        Window w = windows.get(task);
        return w == null ? 0 : w.suppressed.get();
    }

    private static final class Window {
        private long lastLogged = Long.MIN_VALUE / 2;
        private final AtomicInteger suppressed = new AtomicInteger();
    }
}
