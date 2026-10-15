package gt.umg.centinela.simulator.watchdog;

import com.fasterxml.jackson.databind.JsonNode;
import gt.umg.centinela.common.json.Json;
import gt.umg.centinela.simulator.watchdog.WatchdogStatus.Observation;

import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** Sonda HTTP de un endpoint de salud (actuator, Prometheus, Grafana). */
final class HttpProbe {

    private final HttpClient client;
    private final Duration timeout;

    HttpProbe(Duration timeout) {
        this.timeout = timeout;
        this.client = HttpClient.newBuilder().connectTimeout(timeout).build();
    }

    Observation probe(String url) {
        long start = System.nanoTime();
        try {
            HttpResponse<String> response = client.send(HttpRequest.newBuilder(URI.create(url)).timeout(timeout).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            long latency = (System.nanoTime() - start) / 1_000_000;
            return classify(response.statusCode(), response.body(), latency);
        } catch (HttpConnectTimeoutException e) {
            return new Observation(HealthState.DOWN, null, "sin respuesta (timeout de conexion: maquina o red caida)");
        } catch (ConnectException e) {
            return new Observation(HealthState.DOWN, null, "conexion rechazada (la maquina responde, el servicio no)");
        } catch (HttpTimeoutException e) {
            return new Observation(HealthState.DOWN, null, "timeout de lectura");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new Observation(HealthState.DOWN, null, "revision interrumpida");
        } catch (Exception e) {
            return new Observation(HealthState.DOWN, null, e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    static Observation classify(int status, String body, long latencyMs) {
        if (status == 401 || status == 403) {
            return new Observation(HealthState.UP, latencyMs, "responde (requiere autenticacion)");
        }
        JsonNode json = parse(body);
        String reported = json == null ? null : json.path("status").asText(null);
        if (status >= 200 && status < 300) {
            if (reported != null && !reported.equalsIgnoreCase("UP") && !reported.equalsIgnoreCase("ok")) {
                return new Observation(HealthState.DEGRADED, latencyMs, "status " + reported + downComponents(json));
            }
            return new Observation(HealthState.UP, latencyMs, "UP");
        }
        String what = "HTTP " + status + (reported == null ? "" : " status " + reported);
        return new Observation(HealthState.DEGRADED, latencyMs, what + downComponents(json));
    }

    private static JsonNode parse(String body) {
        if (body == null || body.isBlank() || body.charAt(0) != '{') {
            return null;
        }
        try {
            return Json.MAPPER.readTree(body);
        } catch (Exception e) {
            return null;
        }
    }

    private static String downComponents(JsonNode json) {
        if (json == null || !json.has("components")) {
            return "";
        }
        List<String> down = new ArrayList<>();
        Iterator<Map.Entry<String, JsonNode>> it = json.get("components").fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> c = it.next();
            String s = c.getValue().path("status").asText("");
            if (!s.isEmpty() && !s.equals("UP")) {
                down.add(c.getKey() + " " + s);
            }
        }
        return down.isEmpty() ? "" : " (" + String.join(", ", down) + ")";
    }
}
