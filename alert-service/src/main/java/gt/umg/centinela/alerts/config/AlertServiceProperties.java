package gt.umg.centinela.alerts.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.List;

/**
 * Configuracion del alert-service (prefijo {@code alerts}).
 *
 * @param plateHmacSecret   misma llave que la ingesta: permite seudonimizar la lista de placas robadas
 * @param watchlistPlates   placas robadas iniciales (en claro solo en la configuracion; se guardan seudonimizadas)
 * @param plateRetention    tiempo que se conservan seudonimos de placas en eventos y alertas no confirmadas
 * @param alertExpiry       alertas abiertas sin atencion pasan a EXPIRED tras este tiempo
 * @param slo               objetivos de nivel de servicio
 * @param security          usuarios de la API y la consola
 */
@ConfigurationProperties(prefix = "alerts")
public record AlertServiceProperties(
        String plateHmacSecret,
        List<String> watchlistPlates,
        Duration plateRetention,
        Duration alertExpiry,
        Slo slo,
        Security security) {

    /**
     * @param latencyThresholdMs latencia maxima evento -> alerta (2000 ms segun el enunciado)
     * @param targetRatio        proporcion de alertas que debe cumplir el umbral (0.95 = p95)
     * @param window             ventana de medicion
     * @param freshnessThresholdMs retraso maximo aceptable entre un evento y su almacenamiento
     */
    public record Slo(long latencyThresholdMs, double targetRatio, Duration window, long freshnessThresholdMs) {
    }

    public record Security(String operatorUser, String operatorPassword, String adminUser, String adminPassword) {
    }
}
