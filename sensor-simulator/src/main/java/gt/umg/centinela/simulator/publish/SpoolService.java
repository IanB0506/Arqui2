package gt.umg.centinela.simulator.publish;

import com.fasterxml.jackson.databind.JsonNode;
import gt.umg.centinela.common.EventHeaders;
import gt.umg.centinela.common.json.Json;
import gt.umg.centinela.common.kafka.FailureClassifier;
import gt.umg.centinela.common.spool.DiskSpool;
import gt.umg.centinela.common.spool.SpoolEntry;
import gt.umg.centinela.common.spool.SpoolOptions;
import gt.umg.centinela.common.spool.SpoolStats;
import gt.umg.centinela.simulator.config.SimulatorProperties;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.ToDoubleFunction;

/**
 * Dos spools en disco ("store and forward", ADR-006):
 * <ul>
 *   <li>{@code critical.wal}: eventos criticos con escritura anticipada y fsync, sin vencimiento.
 *       Si se llena se rechaza lo nuevo (y se cuenta como perdido): nunca se borra un critico.</li>
 *   <li>{@code telemetry.wal}: telemetria util, sin fsync, vence a los 15 min y, si se llena,
 *       descarta lo mas viejo.</li>
 * </ul>
 * Cada 10 s (o apenas el vigilante ve volver a Kafka) se reenvia primero lo critico y despues la
 * telemetria, a ritmo acotado para que la puesta al dia no parezca una sobrecarga. Los reenvios
 * conservan como timestamp la hora del primer intento: la ingesta mide la edad del evento contra ella.
 */
@Component
public class SpoolService {

    private static final Logger log = LoggerFactory.getLogger(SpoolService.class);
    private static final int CRITICAL_BATCHES_PER_ROUND = 20;
    private static final int TELEMETRY_BATCHES_PER_ROUND = 60;

    private final KafkaTemplate<String, String> kafka;
    private final TaskScheduler scheduler;
    private final SimulatorProperties.Spool config;
    private final DiskSpool critical;
    private final DiskSpool telemetry;
    private final ReentrantLock resendLock = new ReentrantLock();

    public SpoolService(SimulatorProperties properties, KafkaTemplate<String, String> kafka, MeterRegistry metrics,
                        TaskScheduler scheduler) {
        this.kafka = kafka;
        this.scheduler = scheduler;
        this.config = properties.getSpool();
        Path dir = Path.of(properties.getSpoolDir());
        try {
            this.critical = new DiskSpool(new SpoolOptions("critical", dir.resolve("critical.wal"),
                    config.isCriticalFsync(), config.getCriticalMaxSize().toBytes(), null,
                    SpoolOptions.Overflow.REJECT_NEW, config.getMaxAttempts()), FailureClassifier::isPermanent,
                    Clock.systemUTC());
            this.telemetry = new DiskSpool(SpoolOptions.bestEffort("telemetry", dir.resolve("telemetry.wal"),
                    config.getTelemetryMaxSize().toBytes(), config.getTelemetryMaxAge(), false, config.getMaxAttempts()),
                    FailureClassifier::isPermanent, Clock.systemUTC());
            importLegacy(dir.resolve("pending-events.tsv"));
        } catch (IOException e) {
            throw new UncheckedIOException("No se pudo abrir el spool en " + dir, e);
        }
        for (DiskSpool spool : List.of(critical, telemetry)) {
            String name = spool.name();
            Gauge.builder("simulator.spool.pending", spool, DiskSpool::pending).tag("spool", name).register(metrics);
            counter(metrics, "simulator.spool.resent", spool, s -> s.stats().resent());
            counter(metrics, "simulator.spool.rejected", spool, s -> s.stats().rejected());
            counter(metrics, "simulator.spool.expired", spool, s -> s.stats().expired());
            counter(metrics, "simulator.spool.evicted", spool, s -> s.stats().evicted());
            counter(metrics, "simulator.spool.invalid.lines", spool, s -> s.stats().invalidLines());
        }
    }

    private static void counter(MeterRegistry metrics, String name, DiskSpool spool, ToDoubleFunction<DiskSpool> f) {
        FunctionCounter.builder(name, spool, f).tag("spool", spool.name()).register(metrics);
    }

    /** Escritura anticipada de un critico: se llama ANTES de enviarlo. */
    public boolean writeAhead(SpoolEntry entry) {
        return critical.append(entry, true);
    }

    public void confirmed(SpoolEntry entry) {
        critical.ack(entry.id());
    }

    public void sendFailed(SpoolEntry entry, Throwable error) {
        critical.nack(entry.id(), error);
    }

    /** Ultimo recurso para un critico cuya escritura anticipada habia fallado. */
    public boolean storeCritical(SpoolEntry entry) {
        return critical.append(entry, false);
    }

    public boolean storeTelemetry(SpoolEntry entry) {
        return telemetry.append(entry, false);
    }

    /** Mensaje Kafka con el timestamp y los headers de la hora original del evento. */
    public static ProducerRecord<String, String> toRecord(SpoolEntry entry, boolean fromSpool) {
        ProducerRecord<String, String> record = new ProducerRecord<>(entry.topic(), null, entry.createdAtMs(),
                entry.key(), entry.payload());
        record.headers().add(EventHeaders.FIRST_ATTEMPT,
                Long.toString(entry.createdAtMs()).getBytes(StandardCharsets.UTF_8));
        if (fromSpool) {
            record.headers().add(EventHeaders.SPOOLED, "true".getBytes(StandardCharsets.UTF_8));
        }
        return record;
    }

    @Scheduled(fixedDelayString = "${simulator.spool.resend-interval-ms:10000}", initialDelay = 5_000)
    public void resend() {
        if (!resendLock.tryLock()) {
            return;
        }
        try {
            DiskSpool.ResendResult c = critical.resend(this::sendFromSpool, config.getResendBatch(),
                    config.getResendTimeout(), CRITICAL_BATCHES_PER_ROUND);
            DiskSpool.ResendResult t = new DiskSpool.ResendResult(0, 0, telemetry.pending(), false);
            if (!c.stalled()) {
                t = resendTelemetry();
            } else {
                telemetry.expire();
            }
            critical.compactIfNeeded();
            telemetry.compactIfNeeded();
            if (c.sent() > 0 || t.sent() > 0) {
                log.info("Spool: {} criticos y {} de telemetria reenviados; pendientes {} / {}",
                        c.sent(), t.sent(), c.remaining(), t.remaining());
            }
        } catch (RuntimeException e) {
            log.warn("Spool: error en la ronda de reenvio: {}", e.toString());
        } finally {
            resendLock.unlock();
        }
    }

    /** Drena la telemetria a ritmo acotado (por defecto 500 eventos/s). */
    private DiskSpool.ResendResult resendTelemetry() {
        int batch = config.getResendBatch();
        long pauseMs = config.getTelemetryResendRate() > 0 ? batch * 1000L / config.getTelemetryResendRate() : 0;
        int sent = 0;
        int failed = 0;
        DiskSpool.ResendResult r = new DiskSpool.ResendResult(0, 0, telemetry.pending(), false);
        for (int i = 0; i < TELEMETRY_BATCHES_PER_ROUND && telemetry.pending() > 0; i++) {
            r = telemetry.resend(this::sendFromSpool, batch, config.getResendTimeout(), 1);
            sent += r.sent();
            failed += r.failed();
            if (r.stalled() || r.remaining() == 0) {
                break;
            }
            sleep(pauseMs);
        }
        return new DiskSpool.ResendResult(sent, failed, telemetry.pending(), r.stalled());
    }

    /** El vigilante llama a esto cuando Kafka vuelve: no se espera la siguiente ronda de 10 s. */
    public void resendSoon() {
        scheduler.schedule(this::resend, Instant.now());
    }

    private CompletableFuture<?> sendFromSpool(SpoolEntry entry) {
        return kafka.send(toRecord(entry, true));
    }

    public long pendingCount() {
        return critical.pending();
    }

    public long telemetryPending() {
        return telemetry.pending();
    }

    public Map<String, Object> status() {
        Map<String, Object> status = new LinkedHashMap<>();
        for (DiskSpool spool : List.of(critical, telemetry)) {
            SpoolStats s = spool.stats();
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("pendientes", s.pending() + s.inFlight());
            m.put("enVuelo", s.inFlight());
            m.put("bytes", s.bytes());
            m.put("masAntiguoSegundos", s.oldestAgeSeconds());
            m.put("guardados", s.stored());
            m.put("reenviados", s.resent());
            m.put("vencidos", s.expired());
            m.put("descartadosPorTope", s.evicted());
            m.put("rechazadosPorTope", s.refused());
            m.put("apartados", s.rejected());
            m.put("lineasInvalidas", s.invalidLines());
            m.put("erroresDeDisco", s.writeErrors());
            m.put("ultimoError", s.lastError());
            m.put("ultimoReenvio", s.lastResendAt());
            m.put("problemasRecientes", spool.recentProblems());
            status.put(spool.name(), m);
        }
        return status;
    }

    public int requeueRejected(String spool) throws IOException {
        return ("telemetry".equals(spool) ? telemetry : critical).requeueRejected();
    }

    @PreDestroy
    public void close() {
        critical.close();
        telemetry.close();
    }

    /** Importa el spool de la version anterior (topic \t llave \t json) como criticos pendientes. */
    private void importLegacy(Path legacy) throws IOException {
        if (!Files.exists(legacy)) {
            return;
        }
        int imported = 0;
        for (String line : Files.readAllLines(legacy, StandardCharsets.UTF_8)) {
            String[] parts = line.split("\t", 3);
            if (parts.length < 3) {
                continue;
            }
            try {
                JsonNode node = Json.MAPPER.readTree(parts[2]);
                String id = node.path("event_id").asText(java.util.UUID.randomUUID().toString());
                long createdAt = node.hasNonNull("timestamp")
                        ? Instant.parse(node.get("timestamp").asText()).toEpochMilli()
                        : Files.getLastModifiedTime(legacy).toMillis();
                if (critical.append(new SpoolEntry(id, parts[0], parts[1], createdAt, 0, parts[2]), false)) {
                    imported++;
                }
            } catch (IOException | RuntimeException e) {
                log.warn("Spool: linea del archivo anterior ignorada: {}", e.getMessage());
            }
        }
        Files.delete(legacy);
        log.info("Spool: {} eventos criticos importados del archivo anterior {}", imported, legacy.getFileName());
    }

    private static void sleep(long ms) {
        if (ms <= 0) {
            return;
        }
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
