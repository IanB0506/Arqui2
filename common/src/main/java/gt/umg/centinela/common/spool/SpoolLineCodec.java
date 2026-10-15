package gt.umg.centinela.common.spool;

import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32C;

/**
 * Formato del diario del spool, una linea por registro:
 * <pre>
 *   P \t id \t topic \t llave (vacia = null) \t createdAtMs \t attempts \t crc32c del payload \t payload
 *   A \t id      mensaje confirmado (o vencido): ya no se reenvia
 *   F \t id      una falla no transitoria mas
 * </pre>
 * El CRC detecta lineas truncadas por un corte de luz o corruptas aunque conserven los campos:
 * una linea rota nunca llega a Kafka.
 */
final class SpoolLineCodec {

    sealed interface Parsed permits Put, Ack, Fail, Invalid {
    }

    record Put(SpoolEntry entry) implements Parsed {
    }

    record Ack(String id) implements Parsed {
    }

    record Fail(String id) implements Parsed {
    }

    record Invalid(String reason) implements Parsed {
    }

    private SpoolLineCodec() {
    }

    static String put(SpoolEntry e) {
        return "P\t" + e.id() + '\t' + e.topic() + '\t' + (e.key() == null ? "" : e.key()) + '\t' + e.createdAtMs()
                + '\t' + e.attempts() + '\t' + crc(e.payload()) + '\t' + e.payload();
    }

    static String ack(String id) {
        return "A\t" + id;
    }

    static String fail(String id) {
        return "F\t" + id;
    }

    static Parsed parse(String line) {
        if (line.isEmpty()) {
            return new Invalid("linea vacia");
        }
        if (line.startsWith("A\t") || line.startsWith("F\t")) {
            String id = line.substring(2);
            if (id.isEmpty() || id.indexOf('\t') >= 0) {
                return new Invalid("id invalido");
            }
            return line.charAt(0) == 'A' ? new Ack(id) : new Fail(id);
        }
        if (!line.startsWith("P\t")) {
            return new Invalid("tipo de registro desconocido");
        }
        String[] f = line.split("\t", 8);
        if (f.length != 8) {
            return new Invalid("faltan campos");
        }
        try {
            if (!crc(f[7]).equals(f[6])) {
                return new Invalid("CRC distinto (linea truncada o corrupta)");
            }
            SpoolEntry entry = new SpoolEntry(f[1], f[2], f[3].isEmpty() ? null : f[3], Long.parseLong(f[4]),
                    Integer.parseInt(f[5]), f[7]);
            return new Put(entry);
        } catch (IllegalArgumentException e) {
            return new Invalid(e.getMessage());
        }
    }

    static String crc(String payload) {
        CRC32C crc = new CRC32C();
        crc.update(payload.getBytes(StandardCharsets.UTF_8));
        return Long.toHexString(crc.getValue());
    }
}
