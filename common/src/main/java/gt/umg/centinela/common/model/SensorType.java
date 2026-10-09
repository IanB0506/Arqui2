package gt.umg.centinela.common.model;

import gt.umg.centinela.common.Topics;

/** Tipos de sensores urbanos simulados (seccion 2.1 del enunciado). */
public enum SensorType {
    PANIC("Boton de panico", Topics.RAW_PANIC),
    LPR("Camara LPR", Topics.RAW_LPR),
    SPEED("Sensor de velocidad", Topics.RAW_SPEED),
    ACOUSTIC("Sensor acustico", Topics.RAW_ACOUSTIC),
    CITIZEN_REPORT("Reporte ciudadano", Topics.RAW_CITIZEN);

    private final String label;
    private final String rawTopic;

    SensorType(String label, String rawTopic) {
        this.label = label;
        this.rawTopic = rawTopic;
    }

    public String label() {
        return label;
    }

    public String rawTopic() {
        return rawTopic;
    }

    public static SensorType fromRawTopic(String topic) {
        for (SensorType type : values()) {
            if (type.rawTopic.equals(topic)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Topic desconocido: " + topic);
    }
}
