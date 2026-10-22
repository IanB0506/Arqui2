package gt.umg.centinela.ingestion.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Gateway REST de la app movil (prefijo {@code ingestion.gateway}).
 *
 * @param spoolDir         carpeta del spool en disco del gateway (en Docker, un volumen)
 * @param ackTimeout       cuanto se espera la confirmacion de Kafka antes de guardar en disco y
 *                         responder 202 EN_COLA
 * @param kafkaDownBackoff tras una falla, durante este tiempo las peticiones van directo al spool
 * @param maxAttempts      fallas NO transitorias antes de apartar un evento a .rejected
 * @param maxSizeMb        tope del spool; si se llena se responde 503 (nunca 202 sin durabilidad)
 * @param fsync            fsync de cada evento guardado en el spool
 */
@ConfigurationProperties(prefix = "ingestion.gateway")
public record GatewayProperties(
        String spoolDir,
        Duration ackTimeout,
        Duration kafkaDownBackoff,
        int maxAttempts,
        int maxSizeMb,
        boolean fsync) {
}
