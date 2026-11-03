package gt.umg.centinela.common.spool;

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
