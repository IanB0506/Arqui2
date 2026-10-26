package gt.umg.centinela.common.spool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

/**
 * Spool en disco para no perder mensajes cuando Kafka no esta disponible (ADR-006).
 *
 * <p>Es un diario de solo agregar ({@link SpoolLineCodec}) mas un indice en memoria:
 * <ul>
 *   <li><b>Escritura anticipada (write-ahead):</b> el productor guarda el evento critico con
 *       {@link #append(SpoolEntry, boolean)} ANTES de enviarlo. Con {@code fsync} el metodo solo
 *       devuelve true cuando el dato ya esta en disco. Si el proceso muere un instante despues, el
 *       evento se reenvia al reiniciar.</li>
 *   <li><b>fsync agrupado:</b> los hilos que escribieron mientras otro hacia {@code force()} quedan
 *       cubiertos por ese mismo {@code force()}: un fsync por grupo, no por evento.</li>
 *   <li><b>Confirmacion sin fsync:</b> {@link #ack(String)} marca el envio como confirmado sin esperar
 *       al disco. Si un corte de luz pierde esa marca, el evento se reenvia y la deduplicacion por
 *       event_id lo absorbe (al menos una vez).</li>
 *   <li><b>Fallas:</b> las transitorias (Kafka caido) nunca agotan los intentos. Una falla cuenta como
 *       intento si es permanente o si, en el mismo lote, otros mensajes si salieron: entonces el
 *       problema es de esa linea y no debe bloquear el spool para siempre. Al llegar al tope, el
 *       mensaje se aparta a {@code <archivo>.rejected}.</li>
 *   <li><b>Compactacion atomica:</b> lo vivo se escribe a un temporal con fsync y se renombra sobre el
 *       diario. Un corte a mitad deja el diario anterior intacto.</li>
 * </ul>
 * Es seguro para varios hilos: un lock protege indice y archivo y un monitor aparte coordina el fsync
 * agrupado (orden de adquisicion: primero el monitor, luego el lock). Nunca se espera a Kafka con el
 * lock tomado.
 */
public final class DiskSpool implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(DiskSpool.class);
    private static final int MAX_PROBLEMS = 20;
    private static final long MIN_COMPACT_FILE_BYTES = 1024 * 1024;

    /** Envia un mensaje y devuelve un future que se completa cuando Kafka lo confirma. */
    public interface Sender {
        CompletableFuture<?> send(SpoolEntry entry);
    }

    /** Resultado de una ronda de reenvio; {@code stalled} indica que Kafka sigue sin responder. */
    public record ResendResult(int sent, int failed, int remaining, boolean stalled) {
    }

    private static final class Slot {
        SpoolEntry entry;
        boolean inFlight;
        int bytes;

        Slot(SpoolEntry entry, int bytes) {
            this.entry = entry;
            this.bytes = bytes;
        }
    }

    private final SpoolOptions options;
    private final Predicate<Throwable> permanentError;
    private final Clock clock;
    private final Path file;
    private final Path tmpFile;
    private final Path rejectedFile;
    private final ReentrantLock lock = new ReentrantLock();
    private final Object syncMonitor = new Object();
    private final LinkedHashMap<String, Slot> index = new LinkedHashMap<>();
    private final ArrayDeque<String> problems = new ArrayDeque<>();

    private FileChannel channel;
    private boolean closed;
    private long bytes;
    private long obsoleteLines;
    private long writtenSeq;
    private long syncedSeq;        // protegido por syncMonitor
    private long stored;
    private long acked;
    private long resent;
    private long expired;
    private long evicted;
    private long refused;
    private long rejected;
    private long invalidLines;
    private long writeErrors;
    private long syncs;
    private String lastError;
    private Instant lastResendAt;

    public DiskSpool(SpoolOptions options, Predicate<Throwable> permanentError, Clock clock) throws IOException {
        this.options = options;
        this.permanentError = permanentError;
        this.clock = clock;
        this.file = options.file().toAbsolutePath();
        this.tmpFile = file.resolveSibling(file.getFileName() + ".tmp");
        this.rejectedFile = file.resolveSibling(file.getFileName() + ".rejected");
        Path dir = file.getParent();
        if (dir != null) {
            Files.createDirectories(dir);
        }
        Files.deleteIfExists(tmpFile); // compactacion interrumpida: el diario original sigue intacto
        lock.lock();
        try {
            recover();
            channel = openAppend();
        } finally {
            lock.unlock();
        }
        expire();
        compact();
        if (!index.isEmpty()) {
            log.info("Spool {}: {} mensajes pendientes recuperados de {}", options.name(), index.size(), file);
        }
    }

    // ------------------------------------------------------------------ escritura

    /**
     * Guarda un mensaje. Es idempotente por id.
     *
     * @param inFlight true si el llamador lo va a enviar ahora mismo (write-ahead); asi el reenvio
     *                 periodico no lo duplica mientras espera la confirmacion
     * @return true solo si el mensaje quedo guardado (y en disco, si hay fsync); false si no cabe o
     *         el disco fallo: el llamador debe saber que no es durable
     */
    public boolean append(SpoolEntry entry, boolean inFlight) {
        long seq;
        lock.lock();
        try {
            if (closed) {
                return false;
            }
            Slot existing = index.get(entry.id());
            if (existing != null) {
                existing.inFlight = existing.inFlight || inFlight;
                return true;
            }
            byte[] data = (SpoolLineCodec.put(entry) + "\n").getBytes(StandardCharsets.UTF_8);
            if (!makeRoom(data.length)) {
                refused++;
                note("spool lleno: se rechazo " + entry.id());
                return false;
            }
            if (!write(data)) {
                return false;
            }
            Slot slot = new Slot(entry, data.length);
            slot.inFlight = inFlight;
            index.put(entry.id(), slot);
            bytes += data.length;
            stored++;
            seq = ++writtenSeq;
        } finally {
            lock.unlock();
        }
        if (options.fsync()) {
            try {
                syncUpTo(seq);
            } catch (IOException e) {
                lock.lock();
                try {
                    writeErrors++;
                    note("fsync: " + e.getMessage());
                } finally {
                    lock.unlock();
                }
                return false;
            }
        }
        return true;
    }

    /** Envio confirmado por Kafka: deja de estar pendiente. */
    public void ack(String id) {
        lock.lock();
        try {
            Slot slot = index.remove(id);
            if (slot == null) {
                return;
            }
            bytes -= slot.bytes;
            acked++;
            if (!closed) {
                write((SpoolLineCodec.ack(id) + "\n").getBytes(StandardCharsets.UTF_8));
                obsoleteLines += 2;
            }
        } finally {
            lock.unlock();
        }
    }

    /** Envio fallido: vuelve a quedar pendiente. Cuenta como intento solo si la falla es permanente. */
    public void nack(String id, Throwable error) {
        nack(id, error, permanentError.test(error));
    }

    private void nack(String id, Throwable error, boolean countAttempt) {
        lock.lock();
        try {
            Slot slot = index.get(id);
            if (slot == null) {
                return;
            }
            slot.inFlight = false;
            lastError = describe(error);
            if (!countAttempt) {
                return;
            }
            slot.entry = slot.entry.withAttempts(slot.entry.attempts() + 1);
            if (!closed) {
                write((SpoolLineCodec.fail(id) + "\n").getBytes(StandardCharsets.UTF_8));
                obsoleteLines++;
            }
            if (slot.entry.attempts() >= options.maxAttempts()) {
                quarantine(id, slot, describe(error));
            }
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------------ reenvio

    /**
     * Reenvia lo pendiente en lotes, en orden de llegada. Si un lote completo falla, la ronda termina
     * ({@code stalled}) para no insistir con Kafka caido. Los envios que no terminan dentro del plazo
     * siguen "en vuelo" y se confirman o reintentan cuando lleguen, sin duplicarlos.
     */
    public ResendResult resend(Sender sender, int batchSize, Duration batchTimeout, int maxBatches) {
        expire();
        int sent = 0;
        int failed = 0;
        boolean stalled = false;
        for (int b = 0; b < maxBatches && !stalled; b++) {
            List<SpoolEntry> batch = takeBatch(batchSize);
            if (batch.isEmpty()) {
                break;
            }
            List<CompletableFuture<?>> futures = new ArrayList<>(batch.size());
            for (SpoolEntry entry : batch) {
                CompletableFuture<?> future;
                try {
                    future = sender.send(entry);
                    if (future == null) {
                        future = CompletableFuture.failedFuture(new IllegalStateException("el envio no devolvio future"));
                    }
                } catch (RuntimeException e) {
                    future = CompletableFuture.failedFuture(e);
                }
                futures.add(future);
            }
            try {
                CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                        .get(batchTimeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (TimeoutException | ExecutionException e) {
                // se revisa cada envio por separado
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            int ok = 0;
            int pendingInBatch = 0;
            List<Integer> failedIdx = new ArrayList<>();
            List<Throwable> errors = new ArrayList<>();
            for (int i = 0; i < futures.size(); i++) {
                CompletableFuture<?> future = futures.get(i);
                String id = batch.get(i).id();
                if (!future.isDone()) {
                    pendingInBatch++;
                    future.whenComplete((r, e) -> {
                        if (e == null) {
                            ack(id);
                        } else {
                            nack(id, unwrap(e));
                        }
                    });
                    continue;
                }
                try {
                    future.join();
                    ack(id);
                    ok++;
                } catch (CompletionException | CancellationException e) {
                    failedIdx.add(i);
                    errors.add(unwrap(e));
                }
            }
            for (int k = 0; k < failedIdx.size(); k++) {
                Throwable error = errors.get(k);
                nack(batch.get(failedIdx.get(k)).id(), error, permanentError.test(error) || ok > 0);
            }
            sent += ok;
            failed += failedIdx.size();
            if (ok == 0 && (!failedIdx.isEmpty() || pendingInBatch > 0)) {
                stalled = true;
            }
        }
        lock.lock();
        try {
            lastResendAt = clock.instant();
            resent += sent;
            return new ResendResult(sent, failed, index.size(), stalled);
        } finally {
            lock.unlock();
        }
    }

    private List<SpoolEntry> takeBatch(int size) {
        List<SpoolEntry> batch = new ArrayList<>(size);
        lock.lock();
        try {
            for (Slot slot : index.values()) {
                if (batch.size() >= size) {
                    break;
                }
                if (!slot.inFlight) {
                    slot.inFlight = true;
                    batch.add(slot.entry);
                }
            }
        } finally {
            lock.unlock();
        }
        return batch;
    }

    /** Quita lo pendiente que supero {@code maxAge}. Devuelve cuantos vencieron. */
    public int expire() {
        if (options.maxAge() == null) {
            return 0;
        }
        long limit = clock.millis() - options.maxAge().toMillis();
        int count = 0;
        lock.lock();
        try {
            Iterator<java.util.Map.Entry<String, Slot>> it = index.entrySet().iterator();
            while (it.hasNext()) {
                java.util.Map.Entry<String, Slot> e = it.next();
                Slot slot = e.getValue();
                if (!slot.inFlight && slot.entry.createdAtMs() < limit) {
                    it.remove();
                    bytes -= slot.bytes;
                    expired++;
                    count++;
                    if (!closed) {
                        write((SpoolLineCodec.ack(e.getKey()) + "\n").getBytes(StandardCharsets.UTF_8));
                        obsoleteLines += 2;
                    }
                }
            }
        } finally {
            lock.unlock();
        }
        return count;
    }

    /** Devuelve al spool lo apartado en {@code .rejected}, con los intentos en cero. */
    public int requeueRejected() throws IOException {
        List<SpoolEntry> entries = new ArrayList<>();
        lock.lock();
        try {
            if (!Files.exists(rejectedFile)) {
                return 0;
            }
            List<String> keep = new ArrayList<>();
            for (String raw : Files.readAllLines(rejectedFile, StandardCharsets.UTF_8)) {
                String[] parts = raw.split("\t", 3);
                if (parts.length == 3 && SpoolLineCodec.parse(parts[2]) instanceof SpoolLineCodec.Put put) {
                    entries.add(put.entry().withAttempts(0));
                } else {
                    keep.add(raw);
                }
            }
            writeAtomically(rejectedFile, keep.isEmpty() ? "" : String.join("\n", keep) + "\n");
        } finally {
            lock.unlock();
        }
        int count = 0;
        for (SpoolEntry entry : entries) {
            if (append(entry, false)) {
                count++;
            } else {
                appendRejected("no cupo al reencolar", SpoolLineCodec.put(entry));
            }
        }
        return count;
    }

    // ------------------------------------------------------------------ compactacion

    /** Compacta solo si vale la pena (muchas lineas obsoletas o archivo grande). */
    public void compactIfNeeded() {
        boolean needed;
        lock.lock();
        try {
            if (closed) {
                return;
            }
            long live = index.size();
            long fileBytes = channelSize();
            needed = obsoleteLines >= Math.max(100, live / 4)
                    || (live == 0 && obsoleteLines > 0)
                    || fileBytes > 2 * Math.max(options.maxBytes(), MIN_COMPACT_FILE_BYTES);
        } finally {
            lock.unlock();
        }
        if (needed) {
            compact();
        }
    }

    /** Reescribe el diario solo con lo vivo, de forma atomica. */
    public void compact() {
        synchronized (syncMonitor) {
            lock.lock();
            try {
                compactLocked();
            } finally {
                lock.unlock();
            }
        }
    }

    private void compactLocked() {
        if (closed) {
            return;
        }
        try {
            if (index.isEmpty()) {
                channel.truncate(0);
                channel.force(true);
                bytes = 0;
            } else {
                long total = 0;
                try (FileChannel tmp = FileChannel.open(tmpFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                        StandardOpenOption.TRUNCATE_EXISTING)) {
                    for (Slot slot : index.values()) {
                        byte[] data = (SpoolLineCodec.put(slot.entry) + "\n").getBytes(StandardCharsets.UTF_8);
                        writeFully(tmp, data);
                        slot.bytes = data.length;
                        total += data.length;
                    }
                    tmp.force(true);
                }
                channel.close(); // en Windows no se puede reemplazar un archivo abierto
                try {
                    Files.move(tmpFile, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                } catch (AtomicMoveNotSupportedException e) {
                    Files.move(tmpFile, file, StandardCopyOption.REPLACE_EXISTING);
                }
                fsyncDirectory();
                channel = openAppend();
                bytes = total;
            }
            obsoleteLines = 0;
            syncedSeq = writtenSeq; // todo lo vivo ya esta en disco
        } catch (IOException e) {
            note("compactacion: " + e.getMessage());
            log.warn("Spool {}: no se pudo compactar ({}); se sigue con el diario actual", options.name(), e.toString());
            try {
                Files.deleteIfExists(tmpFile);
                if (channel == null || !channel.isOpen()) {
                    channel = openAppend();
                }
            } catch (IOException reopen) {
                log.error("Spool {}: no se pudo reabrir el diario: {}", options.name(), reopen.toString());
            }
        }
    }

    // ------------------------------------------------------------------ consulta y cierre

    /** Mensajes guardados que aun no tienen confirmacion de Kafka (incluye los que estan en vuelo). */
    public int pending() {
        lock.lock();
        try {
            return index.size();
        } finally {
            lock.unlock();
        }
    }

    public SpoolStats stats() {
        lock.lock();
        try {
            int inFlight = 0;
            Long oldest = null;
            for (Slot slot : index.values()) {
                if (slot.inFlight) {
                    inFlight++;
                }
                if (oldest == null) {
                    oldest = Math.max(0, (clock.millis() - slot.entry.createdAtMs()) / 1000);
                }
            }
            return new SpoolStats(options.name(), index.size() - inFlight, inFlight, bytes, channelSize(), oldest,
                    stored, acked, resent, expired, evicted, refused, rejected, invalidLines, writeErrors, syncs,
                    lastError, lastResendAt);
        } finally {
            lock.unlock();
        }
    }

    /** Ultimos problemas (lineas invalidas, mensajes apartados, errores de disco), el mas reciente primero. */
    public List<String> recentProblems() {
        lock.lock();
        try {
            return List.copyOf(problems);
        } finally {
            lock.unlock();
        }
    }

    public String name() {
        return options.name();
    }

    @Override
    public void close() {
        synchronized (syncMonitor) {
            lock.lock();
            try {
                if (closed) {
                    return;
                }
                compactLocked();
                try {
                    channel.force(true);
                    channel.close();
                } catch (IOException e) {
                    log.warn("Spool {}: error al cerrar: {}", options.name(), e.toString());
                }
                closed = true;
            } finally {
                lock.unlock();
            }
        }
    }

    // ------------------------------------------------------------------ internos

    private void recover() throws IOException {
        if (!Files.exists(file)) {
            return;
        }
        byte[] data = Files.readAllBytes(file);
        int start = 0;
        int lineNo = 0;
        for (int i = 0; i < data.length; i++) {
            if (data[i] == '\n') {
                lineNo++;
                // los bytes UTF-8 invalidos se reemplazan y luego fallan el CRC
                apply(new String(data, start, i - start, StandardCharsets.UTF_8), lineNo);
                start = i + 1;
            }
        }
        if (start < data.length) {
            lineNo++;
            invalid(lineNo, "ultima linea truncada (corte durante la escritura)",
                    new String(data, start, data.length - start, StandardCharsets.UTF_8));
        }
    }

    private void apply(String line, int lineNo) {
        SpoolLineCodec.Parsed parsed = SpoolLineCodec.parse(line);
        switch (parsed) {
            case SpoolLineCodec.Put put -> {
                if (!index.containsKey(put.entry().id())) {
                    int size = line.getBytes(StandardCharsets.UTF_8).length + 1;
                    index.put(put.entry().id(), new Slot(put.entry(), size));
                    bytes += size;
                }
            }
            case SpoolLineCodec.Ack ack -> {
                Slot slot = index.remove(ack.id());
                if (slot != null) {
                    bytes -= slot.bytes;
                }
            }
            case SpoolLineCodec.Fail fail -> {
                Slot slot = index.get(fail.id());
                if (slot != null) {
                    slot.entry = slot.entry.withAttempts(slot.entry.attempts() + 1);
                    if (slot.entry.attempts() >= options.maxAttempts()) {
                        quarantine(fail.id(), slot, "tope de intentos (al recuperar)");
                    }
                }
            }
            case SpoolLineCodec.Invalid inv -> invalid(lineNo, inv.reason(), line);
        }
    }

    private void invalid(int lineNo, String reason, String line) {
        invalidLines++;
        String preview = line.length() > 200 ? line.substring(0, 200) + "..." : line;
        log.warn("Spool {}: linea {} invalida apartada ({}): {}", options.name(), lineNo, reason, preview);
        note("linea " + lineNo + " invalida: " + reason);
        appendRejected(reason, line);
    }

    private void quarantine(String id, Slot slot, String reason) {
        index.remove(id);
        bytes -= slot.bytes;
        rejected++;
        appendRejected(reason, SpoolLineCodec.put(slot.entry));
        if (!closed && channel != null) {
            write((SpoolLineCodec.ack(id) + "\n").getBytes(StandardCharsets.UTF_8));
            obsoleteLines += 2;
        }
        note("evento " + id + " apartado: " + reason);
        log.error("Spool {}: evento {} (topic {}) apartado a {} tras {} intentos: {}", options.name(), id,
                slot.entry.topic(), rejectedFile.getFileName(), slot.entry.attempts(), reason);
    }

    private void appendRejected(String reason, String line) {
        String record = clock.instant() + "\t" + reason.replace('\t', ' ').replace('\n', ' ') + "\t" + line + "\n";
        try {
            Files.writeString(rejectedFile, record, StandardCharsets.UTF_8, StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND);
        } catch (IOException e) {
            log.error("Spool {}: no se pudo escribir en {}: {}", options.name(), rejectedFile, e.toString());
        }
    }

    /** Hace espacio para {@code size} bytes. Con DROP_OLDEST descarta lo mas viejo que no este en vuelo. */
    private boolean makeRoom(int size) {
        if (bytes + size <= options.maxBytes()) {
            return true;
        }
        if (options.overflow() != SpoolOptions.Overflow.DROP_OLDEST) {
            return false;
        }
        Iterator<java.util.Map.Entry<String, Slot>> it = index.entrySet().iterator();
        while (bytes + size > options.maxBytes() && it.hasNext()) {
            java.util.Map.Entry<String, Slot> e = it.next();
            if (e.getValue().inFlight) {
                continue;
            }
            it.remove();
            bytes -= e.getValue().bytes;
            evicted++;
            write((SpoolLineCodec.ack(e.getKey()) + "\n").getBytes(StandardCharsets.UTF_8));
            obsoleteLines += 2;
        }
        return bytes + size <= options.maxBytes();
    }

    /** Escribe al final del diario; ante un error deja el archivo como estaba (sin lineas a medias). */
    private boolean write(byte[] data) {
        long before = -1;
        try {
            before = channel.size();
            writeFully(channel, data);
            return true;
        } catch (IOException e) {
            writeErrors++;
            note("escritura en disco: " + e.getMessage());
            log.error("Spool {}: no se pudo escribir en {}: {}", options.name(), file, e.toString());
            if (before >= 0) {
                try {
                    channel.truncate(before);
                } catch (IOException ignored) {
                    // la linea a medias fallara el CRC al recuperar
                }
            }
            return false;
        }
    }

    private void syncUpTo(long seq) throws IOException {
        synchronized (syncMonitor) {
            if (syncedSeq >= seq) {
                return; // otro hilo ya hizo el fsync que cubre esta escritura
            }
            long target;
            FileChannel current;
            lock.lock();
            try {
                target = writtenSeq;
                current = channel;
            } finally {
                lock.unlock();
            }
            current.force(false);
            syncedSeq = target;
            lock.lock();
            try {
                syncs++;
            } finally {
                lock.unlock();
            }
        }
    }

    private FileChannel openAppend() throws IOException {
        return FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
    }

    private long channelSize() {
        try {
            return channel == null || !channel.isOpen() ? 0 : channel.size();
        } catch (IOException e) {
            return 0;
        }
    }

    private void fsyncDirectory() {
        Path dir = file.getParent();
        if (dir == null) {
            return;
        }
        try (FileChannel d = FileChannel.open(dir, StandardOpenOption.READ)) {
            d.force(true);
        } catch (IOException | UnsupportedOperationException e) {
            // Windows no permite abrir un directorio: el rename ya es durable en NTFS
        }
    }

    private void writeAtomically(Path target, String content) throws IOException {
        Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
        Files.writeString(tmp, content, StandardCharsets.UTF_8);
        try {
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static void writeFully(FileChannel ch, byte[] data) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(data);
        while (buf.hasRemaining()) {
            ch.write(buf);
        }
    }

    private void note(String message) {
        lastError = message;
        problems.addFirst(clock.instant() + " " + message);
        while (problems.size() > MAX_PROBLEMS) {
            problems.removeLast();
        }
    }

    private static Throwable unwrap(Throwable e) {
        Throwable t = e;
        while ((t instanceof CompletionException || t instanceof ExecutionException) && t.getCause() != null) {
            t = t.getCause();
        }
        return t;
    }

    private static String describe(Throwable e) {
        Throwable root = e;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String msg = root.getClass().getSimpleName() + (root.getMessage() == null ? "" : ": " + root.getMessage());
        return msg.length() > 200 ? msg.substring(0, 200) : msg;
    }
}
