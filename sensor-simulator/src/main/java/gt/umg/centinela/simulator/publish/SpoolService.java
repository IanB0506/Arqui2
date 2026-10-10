package gt.umg.centinela.simulator.publish;

import gt.umg.centinela.simulator.config.SimulatorProperties;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Buffer en disco para eventos criticos (patron "store and forward").
 *
 * <p>Cada linea del archivo es {@code topic \t llave \t json}. Cada 10 segundos se intenta reenviar
 * todo; lo que vuelva a fallar se conserva para el siguiente intento. Como el evento conserva su
 * event_id, si llegara duplicado la ingesta lo descarta (deduplicacion).
 */
@Component
public class SpoolService {

    private static final Logger log = LoggerFactory.getLogger(SpoolService.class);

    private final Path file;
    private final KafkaTemplate<String, String> kafka;
    private final MeterRegistry metrics;

    public SpoolService(SimulatorProperties properties, KafkaTemplate<String, String> kafka, MeterRegistry metrics) {
        this.file = Path.of(properties.getSpoolDir(), "pending-events.tsv");
        this.kafka = kafka;
        this.metrics = metrics;
        metrics.gauge("simulator.spool.pending", this, SpoolService::pendingCount);
    }

    /** @return true si el evento quedo guardado en disco; false si tampoco fue posible (evento perdido). */
    public synchronized boolean store(String topic, String key, String json) {
        try {
            Files.createDirectories(file.getParent());
            Files.writeString(file, topic + "\t" + key + "\t" + json + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            return true;
        } catch (IOException e) {
            log.error("No se pudo escribir en el spool {}: {}", file, e.toString());
            return false;
        }
    }

    @Scheduled(fixedDelay = 10_000)
    public synchronized void resend() {
        if (!Files.exists(file)) {
            return;
        }
        List<String> lines;
        try {
            lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            log.error("No se pudo leer el spool", e);
            return;
        }
        if (lines.isEmpty()) {
            return;
        }
        List<String> failed = new ArrayList<>();
        int sent = 0;
        for (String line : lines) {
            String[] parts = line.split("\t", 3);
            if (parts.length < 3) {
                continue;
            }
            if (!failed.isEmpty()) {
                failed.add(line); // Kafka sigue caido: no insistir con el resto en esta ronda
                continue;
            }
            try {
                kafka.send(parts[0], parts[1], parts[2]).get(5, TimeUnit.SECONDS);
                sent++;
            } catch (Exception e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                failed.add(line);
            }
        }
        try {
            if (failed.isEmpty()) {
                Files.delete(file);
            } else {
                Files.write(file, failed, StandardCharsets.UTF_8);
            }
        } catch (IOException e) {
            log.error("No se pudo actualizar el spool", e);
        }
        if (sent > 0) {
            metrics.counter("simulator.spool.resent").increment(sent);
            log.info("Spool: {} eventos criticos reenviados, {} pendientes", sent, failed.size());
        }
    }

    public synchronized long pendingCount() {
        try {
            if (!Files.exists(file)) {
                return 0;
            }
            try (var lines = Files.lines(file)) {
                return lines.count();
            }
        } catch (IOException e) {
            return -1;
        }
    }
}
