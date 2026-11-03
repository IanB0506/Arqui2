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

@Component
public class AlertServiceAdminClient {
    private final RestClient adminApi;

    public AlertServiceAdminClient(SimulatorProperties properties) {
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

    public long resetAll() {
        return deletedKeys(adminApi.post().uri("/api/admin/correlation/reset").retrieve().body(JsonNode.class));
    }

    public long resetZones(Collection<Integer> zones) {
        String list = zones.stream().map(String::valueOf).collect(Collectors.joining(","));
        return deletedKeys(adminApi.post().uri(u -> u.path("/api/admin/correlation/reset").queryParam("zonas", list).build())
                .retrieve().body(JsonNode.class));
    }

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
