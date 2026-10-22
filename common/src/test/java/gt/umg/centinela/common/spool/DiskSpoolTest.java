package gt.umg.centinela.common.spool;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

class DiskSpoolTest {

    @TempDir
    Path dir;

    private MutableClock clock;
    private Path file;

    /** En las pruebas, "permanente" = IllegalArgumentException; todo lo demas es transitorio. */
    private static final Predicate<Throwable> PERMANENT = e -> e instanceof IllegalArgumentException;

    @BeforeEach
    void setUp() {
        clock = new MutableClock(Instant.parse("2026-10-15T12:00:00Z"));
        file = dir.resolve("critical.wal");
    }

    private DiskSpool critical() throws IOException {
        return new DiskSpool(SpoolOptions.critical("critical", file, 1024 * 1024, 3), PERMANENT, clock);
    }

    private SpoolEntry entry(String id) {
        return new SpoolEntry(id, "sensor.panic.raw", "dev-" + id, clock.millis(), 0, "{\"event_id\":\"" + id + "\"}");
    }

    private static DiskSpool.Sender ok() {
        return e -> CompletableFuture.completedFuture(null);
    }

    private static DiskSpool.Sender failing(Throwable t) {
        return e -> CompletableFuture.failedFuture(t);
    }

    @Test
    void writeAheadYConfirmacionDejanElDiarioVacioTrasCompactar() throws IOException {
        DiskSpool spool = critical();
        assertThat(spool.append(entry("a"), true)).isTrue();
        assertThat(spool.pending()).isEqualTo(1);
        spool.ack("a");
        assertThat(spool.pending()).isZero();
        spool.compactIfNeeded();
        assertThat(Files.size(file)).isZero();
        spool.close();
    }

    @Test
    void losPendientesSobrevivenAUnCorteEnSuOrdenOriginal() throws IOException {
        DiskSpool spool = critical();
        spool.append(entry("a"), true);
        spool.append(entry("b"), true);
        spool.append(entry("c"), true);
        spool.ack("b");
        // "corte de luz": no se llama close(); se abre otra instancia sobre el mismo archivo
        DiskSpool after = critical();
        List<String> resent = new ArrayList<>();
        DiskSpool.ResendResult r = after.resend(e -> {
            resent.add(e.id());
            return CompletableFuture.completedFuture(null);
        }, 10, Duration.ofSeconds(1), 5);
        assertThat(resent).containsExactly("a", "c");
        assertThat(r.sent()).isEqualTo(2);
        assertThat(after.pending()).isZero();
    }

    @Test
    void lineasInvalidasYTruncadasSeApartanSinPerderLasBuenas() throws IOException {
        DiskSpool spool = critical();
        spool.append(entry("ok1"), false);
        spool.close();
        String good = SpoolLineCodec.put(entry("ok2"));
        Files.writeString(file, good.replace("ok2\"}", "OK2\"}") + "\n", StandardOpenOption.APPEND); // CRC distinto
        Files.writeString(file, "P\tsolo\tdos\n", StandardOpenOption.APPEND);                           // faltan campos
        Files.write(file, new byte[]{'P', '\t', (byte) 0xC3, (byte) 0x28, '\n'}, StandardOpenOption.APPEND); // UTF-8 invalido
        Files.writeString(file, good + "\n", StandardOpenOption.APPEND);
        Files.writeString(file, good.substring(0, 20), StandardOpenOption.APPEND);                      // truncada al final

        DiskSpool after = critical();
        assertThat(after.pending()).isEqualTo(2);
        assertThat(after.stats().invalidLines()).isEqualTo(4);
        assertThat(Files.readAllLines(dir.resolve("critical.wal.rejected"))).hasSize(4);
        assertThat(after.recentProblems()).isNotEmpty();
    }

    @Test
    void unaFallaPermanenteSeAparteAlLlegarAlTope() throws IOException {
        DiskSpool spool = critical();
        spool.append(entry("x"), false);
        for (int i = 0; i < 3; i++) {
            spool.resend(failing(new IllegalArgumentException("demasiado grande")), 10, Duration.ofSeconds(1), 1);
        }
        assertThat(spool.pending()).isZero();
        assertThat(spool.stats().rejected()).isEqualTo(1);
        assertThat(Files.readString(dir.resolve("critical.wal.rejected"))).contains("demasiado grande");
    }

    @Test
    void lasFallasTransitoriasNuncaAgotanLosIntentos() throws IOException {
        DiskSpool spool = critical();
        spool.append(entry("x"), false);
        for (int i = 0; i < 10; i++) {
            DiskSpool.ResendResult r = spool.resend(failing(new IllegalStateException("kafka caido")),
                    10, Duration.ofSeconds(1), 1);
            assertThat(r.stalled()).isTrue();
        }
        assertThat(spool.pending()).isEqualTo(1);
        assertThat(spool.stats().rejected()).isZero();
    }

    @Test
    void laFallaDeUnaLineaCuentaSiLasDemasDelLoteSalen() throws IOException {
        DiskSpool spool = critical();
        spool.append(entry("a"), false);
        spool.append(entry("veneno"), false);
        spool.append(entry("c"), false);
        DiskSpool.Sender sender = e -> e.id().equals("veneno")
                ? CompletableFuture.failedFuture(new IllegalStateException("rara"))
                : CompletableFuture.completedFuture(null);
        for (int i = 0; i < 3; i++) {
            spool.resend(sender, 10, Duration.ofSeconds(1), 1);
            spool.append(entry("relleno" + i), false); // para que el lote siga teniendo exitos
        }
        assertThat(spool.stats().rejected()).isEqualTo(1);
    }

    @Test
    void siFallaElLoteCompletoLaRondaTermina() throws IOException {
        DiskSpool spool = critical();
        for (int i = 0; i < 5; i++) {
            spool.append(entry("e" + i), false);
        }
        AtomicInteger calls = new AtomicInteger();
        DiskSpool.ResendResult r = spool.resend(e -> {
            calls.incrementAndGet();
            return CompletableFuture.failedFuture(new IllegalStateException("caido"));
        }, 2, Duration.ofSeconds(1), 10);
        assertThat(r.stalled()).isTrue();
        assertThat(calls.get()).isEqualTo(2);
        assertThat(spool.pending()).isEqualTo(5);
    }

    @Test
    void loQueEstaEnVueloNoSeReenviaYUnaConfirmacionTardiaLoBorra() throws IOException {
        DiskSpool spool = critical();
        spool.append(entry("enVuelo"), true);
        AtomicInteger calls = new AtomicInteger();
        spool.resend(e -> {
            calls.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }, 10, Duration.ofSeconds(1), 1);
        assertThat(calls.get()).isZero();

        spool.append(entry("lento"), false);
        CompletableFuture<Object> slow = new CompletableFuture<>();
        DiskSpool.ResendResult r = spool.resend(e -> slow, 10, Duration.ofMillis(50), 1);
        assertThat(r.stalled()).isTrue();
        spool.resend(e -> {
            calls.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }, 10, Duration.ofSeconds(1), 1);
        assertThat(calls.get()).isZero(); // sigue en vuelo: no se duplica
        slow.complete("ok");
        spool.ack("enVuelo");
        assertThat(spool.pending()).isZero();
    }

    @Test
    void conDropOldestSeDescartaLoMasViejoYConRejectNewSeRechazaLoNuevo() throws IOException {
        int lineSize = (SpoolLineCodec.put(entry("t0")) + "\n").getBytes(StandardCharsets.UTF_8).length;
        DiskSpool telemetry = new DiskSpool(SpoolOptions.bestEffort("telemetry", dir.resolve("t.wal"), lineSize * 3L,
                null, false, 3), PERMANENT, clock);
        for (int i = 0; i < 5; i++) {
            assertThat(telemetry.append(entry("t" + i), false)).isTrue();
        }
        assertThat(telemetry.pending()).isEqualTo(3);
        assertThat(telemetry.stats().evicted()).isEqualTo(2);

        DiskSpool crit = new DiskSpool(SpoolOptions.critical("c", dir.resolve("c.wal"), lineSize * 2L, 3), PERMANENT, clock);
        assertThat(crit.append(entry("t0"), false)).isTrue();
        assertThat(crit.append(entry("t1"), false)).isTrue();
        assertThat(crit.append(entry("t2"), false)).isFalse();
        assertThat(crit.stats().refused()).isEqualTo(1);
    }

    @Test
    void laTelemetriaVencePorAntiguedad() throws IOException {
        DiskSpool telemetry = new DiskSpool(SpoolOptions.bestEffort("telemetry", dir.resolve("t.wal"), 1 << 20,
                Duration.ofMinutes(15), false, 3), PERMANENT, clock);
        telemetry.append(entry("viejo"), false);
        clock.advance(Duration.ofMinutes(10));
        telemetry.append(entry("nuevo"), false);
        clock.advance(Duration.ofMinutes(6));
        assertThat(telemetry.expire()).isEqualTo(1);
        assertThat(telemetry.pending()).isEqualTo(1);
    }

    @Test
    void laCompactacionNoDejaTemporalYElResultadoSeRecupera() throws IOException {
        Files.writeString(dir.resolve("critical.wal.tmp"), "basura de una compactacion interrumpida");
        DiskSpool spool = critical();
        assertThat(Files.exists(dir.resolve("critical.wal.tmp"))).isFalse();
        for (int i = 0; i < 300; i++) {
            spool.append(entry("e" + i), false);
        }
        for (int i = 0; i < 250; i++) {
            spool.ack("e" + i);
        }
        spool.compactIfNeeded();
        assertThat(Files.exists(dir.resolve("critical.wal.tmp"))).isFalse();
        assertThat(Files.readAllLines(file)).hasSize(50);
        assertThat(critical().pending()).isEqualTo(50);
    }

    @Test
    void appendsConcurrentesConFsyncAgrupadoSeRecuperanTodos() throws Exception {
        DiskSpool spool = new DiskSpool(SpoolOptions.critical("critical", file, 64L * 1024 * 1024, 3), PERMANENT, clock);
        int threads = 8;
        int perThread = 200;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger stored = new AtomicInteger();
        for (int t = 0; t < threads; t++) {
            int id = t;
            pool.submit(() -> {
                start.await();
                for (int i = 0; i < perThread; i++) {
                    if (spool.append(entry("h" + id + "-" + i), true)) {
                        stored.incrementAndGet();
                    }
                }
                return null;
            });
        }
        start.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(60, TimeUnit.SECONDS)).isTrue();
        assertThat(stored.get()).isEqualTo(threads * perThread);
        assertThat(spool.stats().syncs()).isLessThanOrEqualTo(threads * perThread);
        assertThat(critical().pending()).isEqualTo(threads * perThread);
    }

    @Test
    void reencolarLoApartadoReiniciaLosIntentos() throws IOException {
        DiskSpool spool = critical();
        spool.append(entry("x"), false);
        for (int i = 0; i < 3; i++) {
            spool.resend(failing(new IllegalArgumentException("rechazado")), 10, Duration.ofSeconds(1), 1);
        }
        assertThat(spool.pending()).isZero();
        assertThat(spool.requeueRejected()).isEqualTo(1);
        assertThat(spool.pending()).isEqualTo(1);
        spool.resend(ok(), 10, Duration.ofSeconds(1), 1);
        assertThat(spool.pending()).isZero();
    }

    @Test
    void appendEsIdempotentePorId() throws IOException {
        DiskSpool spool = critical();
        assertThat(spool.append(entry("a"), false)).isTrue();
        assertThat(spool.append(entry("a"), false)).isTrue();
        assertThat(spool.pending()).isEqualTo(1);
    }
}
