package gt.umg.centinela.common.spool;

/**
 * Un mensaje guardado en el spool.
 *
 * @param id          event_id: unico; sirve para confirmar el envio y para deduplicar
 * @param topic       topic de Kafka de destino
 * @param key         llave del mensaje (puede ser null)
 * @param createdAtMs hora del primer intento de envio; sera el timestamp del mensaje al reenviarlo
 * @param attempts    fallas NO transitorias acumuladas (las transitorias no cuentan)
 * @param payload     JSON del evento, en una sola linea
 */
public record SpoolEntry(String id, String topic, String key, long createdAtMs, int attempts, String payload) {

    public SpoolEntry {
        requireField("id", id, false);
        requireField("topic", topic, false);
        if (key != null) {
            requireField("key", key, true);
        }
        if (payload == null || payload.indexOf('\n') >= 0 || payload.indexOf('\r') >= 0) {
            throw new IllegalArgumentException("El payload del spool debe ser una sola linea");
        }
    }

    public SpoolEntry withAttempts(int value) {
        return new SpoolEntry(id, topic, key, createdAtMs, value, payload);
    }

    private static void requireField(String name, String value, boolean allowEmpty) {
        if (value == null || (!allowEmpty && value.isEmpty())) {
            throw new IllegalArgumentException("Campo vacio en el spool: " + name);
        }
        if (value.indexOf('\t') >= 0 || value.indexOf('\n') >= 0 || value.indexOf('\r') >= 0) {
            throw new IllegalArgumentException("Caracter no permitido en el campo " + name + " del spool");
        }
    }
}
