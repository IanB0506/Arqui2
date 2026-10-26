package gt.umg.centinela.simulator.replay;

import gt.umg.centinela.common.model.SensorType;
import gt.umg.centinela.common.model.raw.CitizenEventType;
import gt.umg.centinela.common.model.raw.PanicAlertType;
import gt.umg.centinela.common.model.raw.ReportOrigin;
import gt.umg.centinela.common.model.raw.SoundType;
import gt.umg.centinela.simulator.generator.EventFactory;
import gt.umg.centinela.simulator.generator.SimulatedEvent;
import gt.umg.centinela.simulator.publish.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;

/**
 * Reproduce eventos desde un CSV predefinido ("CSV predefinidos + Randomizer" del enunciado).
 *
 * <p>Formato: {@code offset_s,sensor,zona,subtipo,valor,placa,fuente}. Los tiempos son relativos al
 * inicio del replay. Con {@code jitter=true} el "randomizer" mueve cada evento +-10 % en el tiempo
 * para que dos replays del mismo archivo no sean identicos.
 */
@Component
public class CsvReplayService {

    private static final Logger log = LoggerFactory.getLogger(CsvReplayService.class);
    private static final Pattern SAFE_NAME = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,80}\\.csv");

    private final EventFactory factory;
    private final EventPublisher publisher;
    private final TaskScheduler scheduler;

    public CsvReplayService(EventFactory factory, EventPublisher publisher, TaskScheduler scheduler) {
        this.factory = factory;
        this.publisher = publisher;
        this.scheduler = scheduler;
    }

    public record ReplayRow(double offsetSeconds, SensorType sensor, int zone, String subtype, Double value,
                            String plate, String source) {
    }

    public record ReplaySummary(String source, int events, double durationSeconds, Instant startedAt) {
    }

    /** @param resource nombre de un CSV incluido en el jar (carpeta replay/ de los recursos del simulador) */
    public ReplaySummary replay(String resource, double speed, boolean jitter) throws IOException {
        List<ReplayRow> rows = parse(open(resource));
        double factor = speed <= 0 ? 1.0 : speed;
        Instant start = Instant.now();
        double maxOffset = 0;
        for (ReplayRow row : rows) {
            double offset = row.offsetSeconds();
            if (jitter && offset > 0) {
                offset *= ThreadLocalRandom.current().nextDouble(0.9, 1.1);
            }
            maxOffset = Math.max(maxOffset, offset / factor);
            scheduler.schedule(() -> publisher.publish(toEvent(row)), start.plusMillis((long) (offset * 1000 / factor)));
        }
        log.info("Replay de {}: {} eventos en {} s", resource, rows.size(), Math.round(maxOffset));
        return new ReplaySummary(resource, rows.size(), maxOffset, start);
    }

    /** Solo CSV incluidos en el jar: un nombre con rutas permitiria leer otros archivos del contenedor. */
    static InputStream open(String resource) throws IOException {
        if (resource == null || !SAFE_NAME.matcher(resource).matches() || resource.contains("..")) {
            throw new IllegalArgumentException("Nombre de CSV invalido: use un archivo de la carpeta replay/ "
                    + "del simulador, p. ej. noche-de-viernes.csv");
        }
        ClassPathResource csv = new ClassPathResource("replay/" + resource);
        if (!csv.exists()) {
            throw new NoSuchElementException("No existe el CSV de replay " + resource);
        }
        return csv.getInputStream();
    }

    static List<ReplayRow> parse(InputStream input) throws IOException {
        List<ReplayRow> rows = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))) {
            String line;
            boolean header = true;
            while ((line = reader.readLine()) != null) {
                if (header) {
                    header = false;
                    continue;
                }
                if (line.isBlank() || line.startsWith("#")) {
                    continue;
                }
                String[] c = line.split(",", -1);
                rows.add(new ReplayRow(
                        Double.parseDouble(c[0].trim()),
                        SensorType.valueOf(c[1].trim().toUpperCase(Locale.ROOT)),
                        Integer.parseInt(c[2].trim()),
                        c.length > 3 ? c[3].trim() : "",
                        c.length > 4 && !c[4].isBlank() ? Double.parseDouble(c[4].trim()) : null,
                        c.length > 5 ? c[5].trim() : "",
                        c.length > 6 ? c[6].trim() : ""));
            }
        }
        return rows;
    }

    private SimulatedEvent toEvent(ReplayRow row) {
        int sensorIndex = ThreadLocalRandom.current().nextInt(1, 3);
        return switch (row.sensor()) {
            case PANIC -> factory.panic(row.zone(), PanicAlertType.valueOf(row.subtype()),
                    row.source().isBlank() ? factory.randomAppDevice() : row.source());
            case LPR -> factory.lpr(row.zone(), sensorIndex,
                    row.plate().isBlank() ? factory.randomPlate() : row.plate(), valueOr(row, 50));
            case SPEED -> factory.speed(row.zone(), sensorIndex, valueOr(row, 50), "CONTRAVIA".equals(row.subtype()));
            case ACOUSTIC -> {
                double probability = valueOr(row, 0.5);
                yield factory.acoustic(row.zone(), sensorIndex, SoundType.valueOf(row.subtype()), 100 + probability * 25, probability);
            }
            case CITIZEN_REPORT -> factory.citizen(row.zone(), CitizenEventType.valueOf(row.subtype()),
                    row.source().isBlank() ? factory.randomCitizenId() : row.source(), ReportOrigin.APP,
                    "Reporte reproducido desde CSV");
        };
    }

    private static double valueOr(ReplayRow row, double fallback) {
        return row.value() == null ? fallback : row.value();
    }
}
