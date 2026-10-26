package gt.umg.centinela.simulator.admin;

import com.fasterxml.jackson.databind.JsonNode;
import gt.umg.centinela.simulator.config.SimulatorProperties;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.net.http.HttpClient;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * API de administracion del alert-service (rol ADMIN; cada llamada queda en la bitacora de auditoria):
 * limpiar el estado temporal del motor de correlacion (no borra eventos ni alertas de la base) e informar
 * eventos criticos que no se pudieron guardar (SLO-2).
 */
@Component
public class AlertServiceAdminClient {

    private final RestClient adminApi;

    public AlertServiceAdminClient(SimulatorProperties properties) {
        // Con timeouts: si el nodo B no responde, falla con un motivo claro en vez de colgarse
        HttpClient http = HttpClient.newBuilder().connectTimeout(properties.getHttpTimeout()).build();
        JdkClientHttpRequestFactory requests = new JdkClientHttpRequestFactory(http);
        requests.setReadTimeout(properties.getHttpTimeout());
        this.adminApi = RestClient.builder()
                .requestFactory(requests)
                .baseUrl(properties.getAlertServiceUrl())
                .defaultHeaders(h -> h.setBasicAuth(properties.getAlertServiceAdminUser(),
                        properties.getAlertServiceAdminPassword()))
                .build();
    }

    /** Ventanas, incidentes abiertos, anclas pendientes y sospechosos de todas las zonas. Devuelve las llaves borradas. */
    public long resetAll() {
        return deletedKeys(adminApi.post().uri("/api/admin/correlation/reset").retrieve().body(JsonNode.class));
    }

    /** Historial reciente de eventos (ventanas) e incidentes abiertos de las zonas indicadas. Devuelve las llaves borradas. */
    public long resetZones(Collection<Integer> zones) {
        String list = zones.stream().map(String::valueOf).collect(Collectors.joining(","));
        return deletedKeys(adminApi.post().uri(u -> u.path("/api/admin/correlation/reset").queryParam("zonas", list).build())
                .retrieve().body(JsonNode.class));
    }

    /** Informa eventos criticos que el simulador no pudo guardar: el alert-service genera una alerta CRITICAL. */
    public void reportLostCriticalEvents(String node, long lost) {
        adminApi.post().uri("/api/admin/slo/lost-critical-events")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("node", node, "lost", lost))
                .retrieve().toBodilessEntity();
    }

    private static long deletedKeys(JsonNode result) {
        return result == null ? 0 : result.path("deletedKeys").asLong();
    }
}
