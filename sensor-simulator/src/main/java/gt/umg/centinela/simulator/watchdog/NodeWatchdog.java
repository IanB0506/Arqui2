package gt.umg.centinela.simulator.watchdog;

import gt.umg.centinela.simulator.config.SimulatorProperties;
import gt.umg.centinela.simulator.publish.SpoolService;
import gt.umg.centinela.simulator.watchdog.WatchdogStatus.GroupStatus;
import gt.umg.centinela.simulator.watchdog.WatchdogStatus.KafkaStatus;
import gt.umg.centinela.simulator.watchdog.WatchdogStatus.NodeBStatus;
import gt.umg.centinela.simulator.watchdog.WatchdogStatus.Observation;
import gt.umg.centinela.simulator.watchdog.WatchdogStatus.TargetStatus;
import gt.umg.centinela.simulator.watchdog.WatchdogStatus.Transition;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Vigilante del nodo B y de Kafka. Corre en los nodos generadores (C/D), fuera del nodo B: si B se
 * cae, alguien lo nota y lo dice (ADR-006).
 *
 * <p>Cada 10 s revisa en paralelo la salud de los servicios de B y consulta a Kafka cuantos brokers
 * responden y, por grupo de consumidores, quien esta consumiendo (y desde que nodo) y cuanto atraso
 * hay. Las transiciones quedan en el log como WARN ("VIGILANTE: NODO B SIN RESPUESTA ..."), en la
 * pagina /vigilante.html, en GET /api/watchdog y como metricas para Prometheus.
 */
@Component
public class NodeWatchdog {

    private static final Logger log = LoggerFactory.getLogger(NodeWatchdog.class);

    private final SimulatorProperties.Watchdog config;
    private final String node;
    private final SpoolService spool;
    private final Clock clock = Clock.systemUTC();
    private final HttpProbe http;
    private final KafkaAdmin kafkaAdmin;
    private final Map<String, HealthTracker> targets = new LinkedHashMap<>();
    private final HealthTracker kafkaTracker;
    private final ArrayDeque<Transition> history = new ArrayDeque<>();
    private final Map<String, Long> previousLag = new HashMap<>();
    private final Map<String, String> groupConditions = new HashMap<>();
    private KafkaProbe kafkaProbe;
    private String nodeBState = "DESCONOCIDO";
    private Instant nodeBSince;
    private volatile WatchdogStatus snapshot;

    public NodeWatchdog(SimulatorProperties properties, KafkaAdmin kafkaAdmin, SpoolService spool, MeterRegistry metrics) {
        this.config = properties.getWatchdog();
        this.node = properties.getNodeName();
        this.spool = spool;
        this.kafkaAdmin = kafkaAdmin;
        this.http = new HttpProbe(config.getTimeout());
        config.getTargets().forEach((name, url) -> {
            if (url != null && !url.isBlank()) {
                targets.put(name, new HealthTracker(name, url, config.getFailuresToDown()));
            }
        });
        this.kafkaTracker = new HealthTracker("kafka", "kafka", config.getFailuresToDown());
        for (HealthTracker t : targets.values()) {
            Gauge.builder("simulator.watchdog.status", t, x -> x.state().value()).tag("target", t.status().name())
                    .register(metrics);
        }
        Gauge.builder("simulator.watchdog.status", kafkaTracker, x -> x.state().value()).tag("target", "kafka")
                .register(metrics);
        this.snapshot = new WatchdogStatus(node, config.isEnabled(), null,
                new NodeBStatus(nodeBState, null, "sin revisar todavia"), List.of(), null, List.of());
    }

    @Scheduled(initialDelay = 5_000, fixedDelayString = "${simulator.watchdog.interval-ms:10000}")
    public void check() {
        if (!config.isEnabled()) {
            return;
        }
        Instant now = clock.instant();
        Map<String, CompletableFuture<Observation>> probes = new LinkedHashMap<>();
        CompletableFuture<KafkaProbe.KafkaObservation> kafkaFuture;
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            targets.forEach((name, tracker) -> probes.put(name,
                    CompletableFuture.supplyAsync(() -> http.probe(tracker.status().url()), executor)));
            kafkaFuture = CompletableFuture.supplyAsync(() -> kafkaProbe().probe(config.getKafkaGroups()), executor);
        }
        probes.forEach((name, future) -> targets.get(name).observe(future.join(), now).ifPresent(this::onTransition));

        KafkaProbe.KafkaObservation k = kafkaFuture.join();
        HealthState kafkaBefore = kafkaTracker.state();
        kafkaTracker.observe(new Observation(k.state(), null, k.detail()), now).ifPresent(t -> {
            onTransition(t);
            if (t.to() == HealthState.UP && kafkaBefore == HealthState.DOWN) {
                spool.resendSoon(); // no esperar a la siguiente ronda de 10 s
            }
        });
        List<GroupStatus> groups = groups(k);
        updateNodeB(now, groups);
        List<TargetStatus> targetStatus = targets.values().stream().map(HealthTracker::status).toList();
        TargetStatus ks = kafkaTracker.status();
        synchronized (history) {
            snapshot = new WatchdogStatus(node, true, now, new NodeBStatus(nodeBState, nodeBSince, nodeBSummary(groups)),
                    targetStatus, new KafkaStatus(ks.state(), ks.since(), k.brokers(), k.detail(), groups),
                    List.copyOf(history));
        }
    }

    private List<GroupStatus> groups(KafkaProbe.KafkaObservation k) {
        List<GroupStatus> result = new ArrayList<>();
        for (Map.Entry<String, KafkaProbe.GroupInfo> e : k.groups().entrySet()) {
            String group = e.getKey();
            KafkaProbe.GroupInfo g = e.getValue();
            Long before = previousLag.get(group);
            String trend = g.lag() == null || before == null ? "-"
                    : g.lag() < before ? "BAJANDO" : g.lag() > before ? "SUBIENDO" : "ESTABLE";
            if (g.lag() != null) {
                previousLag.put(group, g.lag());
            }
            String condition;
            String message;
            if (g.lag() == null) {
                condition = "DESCONOCIDO";
                message = "sin datos";
            } else if (g.consumers().isEmpty() && g.lag() > 0) {
                condition = "SIN_CONSUMIDORES";
                message = g.lag() + " mensajes esperando en Kafka sin nadie que los consuma";
            } else if (g.lag() >= config.getLagWarn()) {
                condition = "ATRASADO";
                message = "atraso de " + g.lag() + " mensajes (" + trend.toLowerCase() + ")";
            } else {
                condition = "OK";
                message = g.consumers().isEmpty() ? "sin consumidores (y sin mensajes pendientes)" : "al dia";
            }
            String previous = groupConditions.put(group, condition);
            if (previous != null && !previous.equals(condition)) {
                log.warn("VIGILANTE: grupo {} paso de {} a {}: {}", group, previous, condition, message);
            }
            result.add(new GroupStatus(group, g.state(), g.consumers().size(), g.consumers(), g.lag(), trend,
                    condition, message));
        }
        return result;
    }

    private void updateNodeB(Instant now, List<GroupStatus> groups) {
        String state = nodeBState(targets.values().stream().map(HealthTracker::state).toList());
        if (!state.equals(nodeBState)) {
            String before = nodeBState;
            Long lasted = nodeBSince == null ? null : Duration.between(nodeBSince, now).toSeconds();
            nodeBState = state;
            nodeBSince = now;
            if (state.equals("OPERATIVO") && !before.equals("DESCONOCIDO")) {
                log.warn("VIGILANTE: nodo B operativo de nuevo{}", lasted == null ? "" : " tras " + human(lasted));
            } else if (!state.equals("OPERATIVO") && !state.equals("DESCONOCIDO")) {
                log.warn("VIGILANTE: NODO B {} desde {}. {}", state.replace('_', ' '), now, nodeBSummary(groups));
            } else {
                log.info("VIGILANTE: nodo B {}", state);
            }
        }
    }

    /** Resumen legible: que esta caido y si la deteccion continua en otro nodo. */
    private String nodeBSummary(List<GroupStatus> groups) {
        List<String> down = new ArrayList<>();
        for (HealthTracker t : targets.values()) {
            TargetStatus s = t.status();
            if (s.state() == HealthState.DOWN || s.state() == HealthState.DEGRADED) {
                down.add(s.name() + " " + s.state());
            }
        }
        StringBuilder sb = new StringBuilder(down.isEmpty() ? "Todos los servicios responden." : "Caidos o degradados: "
                + String.join(", ", down) + ".");
        sb.append(nodeACause());
        for (GroupStatus g : groups) {
            if (g.group().equals("correlation") && !down.isEmpty()) {
                sb.append(g.consumers().isEmpty() ? " La deteccion esta detenida: los eventos esperan en Kafka."
                        : " La deteccion continua en: " + String.join(", ", g.consumers()) + ".");
            }
            if (g.condition().equals("SIN_CONSUMIDORES")) {
                sb.append(" Grupo ").append(g.group()).append(": ").append(g.message()).append('.');
            }
        }
        return sb.toString();
    }

    /**
     * Si lo que falla son las dependencias que viven en el nodo A (Kafka, Redis, la base), lo dice: el
     * nodo B esta vivo y se pondra al dia solo cuando A vuelva. Evita buscar el problema en B.
     */
    private String nodeACause() {
        List<String> signs = new ArrayList<>();
        if (kafkaTracker.state() == HealthState.DOWN) {
            signs.add("Kafka no responde");
        }
        Map<String, Integer> dependencies = new LinkedHashMap<>();
        for (HealthTracker t : targets.values()) {
            TargetStatus s = t.status();
            if (s.state() == HealthState.DEGRADED && s.detail() != null) {
                for (String dependency : List.of("redis", "db")) {
                    if (s.detail().contains(dependency + " DOWN")) {
                        dependencies.merge(dependency, 1, Integer::sum);
                    }
                }
            }
        }
        dependencies.forEach((dependency, n) -> signs.add(dependency + " DOWN en " + n + " servicio(s)"));
        return signs.isEmpty() ? "" : " Causa probable: el nodo A (datos) no responde (" + String.join("; ", signs)
                + "). El nodo B esta vivo y se pone al dia solo cuando A vuelva.";
    }

    static String nodeBState(Collection<HealthState> states) {
        if (states.isEmpty() || states.stream().allMatch(s -> s == HealthState.UNKNOWN)) {
            return "DESCONOCIDO";
        }
        if (states.stream().allMatch(s -> s == HealthState.DOWN)) {
            return "SIN_RESPUESTA";
        }
        if (states.stream().allMatch(s -> s == HealthState.UP)) {
            return "OPERATIVO";
        }
        return "DEGRADADO";
    }

    private void onTransition(Transition t) {
        synchronized (history) {
            history.addFirst(t);
            while (history.size() > config.getHistorySize()) {
                history.removeLast();
            }
        }
        if (t.from() == HealthState.UNKNOWN && t.to() == HealthState.UP) {
            log.info("VIGILANTE: {} disponible", t.target());
            return;
        }
        log.warn("VIGILANTE: {} paso de {} a {}: {}{}", t.target(), t.from(), t.to(), t.detail(),
                t.previousDurationSeconds() == null ? "" : " (el estado anterior duro " + human(t.previousDurationSeconds()) + ")");
    }

    private synchronized KafkaProbe kafkaProbe() {
        if (kafkaProbe == null) {
            kafkaProbe = new KafkaProbe(kafkaAdmin.getConfigurationProperties(), "simulator-watchdog-" + node,
                    config.getTimeout());
        }
        return kafkaProbe;
    }

    public WatchdogStatus snapshot() {
        return snapshot;
    }

    private static String human(long seconds) {
        return seconds >= 60 ? (seconds / 60) + " min " + (seconds % 60) + " s" : seconds + " s";
    }

    @PreDestroy
    public synchronized void close() {
        if (kafkaProbe != null) {
            kafkaProbe.close();
        }
    }
}
