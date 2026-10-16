package gt.umg.centinela.simulator.validation;

import com.fasterxml.jackson.databind.JsonNode;
import gt.umg.centinela.common.model.Severity;
import gt.umg.centinela.simulator.config.SimulatorProperties;
import gt.umg.centinela.simulator.scenario.ExpectedAlert;
import gt.umg.centinela.simulator.scenario.ScenarioCatalog;
import gt.umg.centinela.simulator.scenario.ScenarioDefinition;
import gt.umg.centinela.simulator.scenario.ScenarioExecution;
import gt.umg.centinela.simulator.scenario.ScenarioRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.net.http.HttpClient;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Validacion automatica de correlaciones esperadas (criterio "Validacion de correlaciones
 * esperadas" de la puesta en simulacion).
 *
 * <p>Lanza en paralelo todos los escenarios marcados para validacion, cada uno en su zona, y luego
 * consulta la API del alert-service hasta encontrar cada alerta esperada o agotar el tiempo.
 * El reporte indica que se encontro, con que severidad y con que latencia.
 */
@Service
public class ValidationService {

    private static final Logger log = LoggerFactory.getLogger(ValidationService.class);

    private final ScenarioCatalog catalog;
    private final ScenarioRunner runner;
    private final RestClient alertApi;
    private final RestClient adminApi;
    private final Map<UUID, ValidationReport> reports = new ConcurrentHashMap<>();
    private volatile UUID latest;

    public ValidationService(ScenarioCatalog catalog, ScenarioRunner runner, SimulatorProperties properties) {
        this.catalog = catalog;
        this.runner = runner;
        // Con timeouts: si el nodo B no responde, la validacion falla con un motivo claro en vez de colgarse
        HttpClient http = HttpClient.newBuilder().connectTimeout(properties.getHttpTimeout()).build();
        JdkClientHttpRequestFactory requests = new JdkClientHttpRequestFactory(http);
        requests.setReadTimeout(properties.getHttpTimeout());
        this.alertApi = RestClient.builder()
                .requestFactory(requests)
                .baseUrl(properties.getAlertServiceUrl())
                .defaultHeaders(h -> h.setBasicAuth(properties.getAlertServiceUser(), properties.getAlertServicePassword()))
                .defaultHeader(HttpHeaders.ACCEPT, "application/json")
                .build();
        this.adminApi = RestClient.builder()
                .requestFactory(requests)
                .baseUrl(properties.getAlertServiceUrl())
                .defaultHeaders(h -> h.setBasicAuth(properties.getAlertServiceAdminUser(),
                        properties.getAlertServiceAdminPassword()))
                .build();
    }

    public record ValidationResult(String scenario, String expectedAlert, int zoneId, String minSeverity,
                                   boolean passed, String foundSeverity, UUID alertId, Long latencyMs,
                                   Double detectedAfterSeconds, String detail) {
    }

    public record ValidationReport(UUID id, String status, Instant startedAt, Instant finishedAt,
                                   int passed, int total, String stateReset, List<ValidationResult> results) {
    }

    private record Pending(String scenario, ExpectedAlert expected, Instant startedAt) {
    }

    /**
     * @param resetState si es true, antes de lanzar los escenarios limpia el estado temporal del motor
     *                   (ventanas e incidentes abiertos) para que una corrida anterior no contamine el
     *                   resultado: p. ej. un incidente abierto hace 3 minutos absorberia la nueva evidencia.
     */
    public ValidationReport start(boolean resetState) {
        UUID id = UUID.randomUUID();
        String reset = resetState ? resetCorrelationState() : "no solicitado";
        Instant startedAt = Instant.now();
        List<Pending> pending = new ArrayList<>();
        for (ScenarioDefinition definition : catalog.all()) {
            if (!definition.includeInValidation()) {
                continue;
            }
            ScenarioExecution execution = runner.run(definition.id(), null, 1.0);
            execution.expected().forEach(e -> pending.add(new Pending(definition.id(), e, execution.startedAt())));
        }
        ValidationReport running = new ValidationReport(id, "RUNNING", startedAt, null, 0, pending.size(), reset, List.of());
        reports.put(id, running);
        latest = id;
        CompletableFuture.runAsync(() -> evaluate(id, startedAt, pending, reset));
        return running;
    }

    private String resetCorrelationState() {
        try {
            JsonNode result = adminApi.post().uri("/api/admin/correlation/reset").retrieve().body(JsonNode.class);
            String summary = "ok (" + (result == null ? 0 : result.path("deletedKeys").asLong()) + " llaves eliminadas)";
            log.info("Estado de correlacion reiniciado antes de validar: {}", summary);
            sleep(1500); // margen para que terminen de procesarse eventos en vuelo
            return summary;
        } catch (Exception e) {
            log.warn("No se pudo reiniciar el estado de correlacion: {}", e.getMessage());
            return "fallo: " + e.getMessage();
        }
    }

    public Optional<ValidationReport> find(UUID id) {
        return Optional.ofNullable(reports.get(id));
    }

    public Optional<ValidationReport> latest() {
        return latest == null ? Optional.empty() : find(latest);
    }

    private void evaluate(UUID id, Instant startedAt, List<Pending> pending, String reset) {
        Map<Pending, ValidationResult> results = new ConcurrentHashMap<>();
        int maxWait = pending.stream().mapToInt(p -> p.expected().withinSeconds()).max().orElse(60);
        Instant deadline = startedAt.plusSeconds(maxWait + 5);
        while (results.size() < pending.size() && Instant.now().isBefore(deadline)) {
            sleep(3000);
            for (Pending p : pending) {
                if (!results.containsKey(p)) {
                    findAlert(p).ifPresent(r -> results.put(p, r));
                }
            }
        }
        List<ValidationResult> ordered = new ArrayList<>();
        for (Pending p : pending) {
            ordered.add(results.getOrDefault(p, new ValidationResult(p.scenario(), p.expected().alertType(),
                    p.expected().zoneId(), p.expected().minSeverity(), false, null, null, null, null,
                    "No se encontro la alerta en " + p.expected().withinSeconds() + " s")));
        }
        int passed = (int) ordered.stream().filter(ValidationResult::passed).count();
        String status = passed == ordered.size() ? "PASSED" : "FAILED";
        reports.put(id, new ValidationReport(id, status, startedAt, Instant.now(), passed, ordered.size(), reset, ordered));
        log.info("Validacion {} terminada: {}/{} correlaciones esperadas encontradas", id, passed, ordered.size());
    }

    private Optional<ValidationResult> findAlert(Pending p) {
        try {
            JsonNode alerts = alertApi.get()
                    .uri(u -> u.path("/api/alerts")
                            .queryParam("since", p.startedAt().toString())
                            .queryParam("zoneId", p.expected().zoneId())
                            .queryParam("type", p.expected().alertType())
                            .build())
                    .retrieve()
                    .body(JsonNode.class);
            if (alerts == null || !alerts.isArray() || alerts.isEmpty()) {
                return Optional.empty();
            }
            Severity required = Severity.valueOf(p.expected().minSeverity());
            JsonNode best = null;
            for (JsonNode alert : alerts) {
                if (best == null || Severity.valueOf(alert.path("severity").asText()).ordinal()
                        > Severity.valueOf(best.path("severity").asText()).ordinal()) {
                    best = alert;
                }
            }
            Severity found = Severity.valueOf(best.path("severity").asText());
            if (found.ordinal() < required.ordinal()) {
                return Optional.empty(); // aun no escala a la severidad esperada; seguir esperando
            }
            // updatedAt = momento en que la alerta alcanzo su severidad actual (tras escalar si aplica)
            Instant reached = Instant.parse(best.path("updatedAt").asText());
            double after = Duration.between(p.startedAt(), reached).toMillis() / 1000.0;
            boolean inTime = after <= p.expected().withinSeconds();
            return Optional.of(new ValidationResult(p.scenario(), p.expected().alertType(), p.expected().zoneId(),
                    p.expected().minSeverity(), inTime, found.name(), UUID.fromString(best.path("alertId").asText()),
                    best.path("latencyMs").isNumber() ? best.path("latencyMs").asLong() : null, after,
                    inTime ? "OK" : "Encontrada fuera de tiempo"));
        } catch (Exception e) {
            log.warn("No se pudo consultar alert-service: {}", e.getMessage());
            return Optional.empty();
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
