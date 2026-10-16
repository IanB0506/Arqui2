package gt.umg.centinela.common;

/**
 * Nombres de los topics de Kafka usados por todo el sistema.
 *
 * <p>Cada tipo de sensor tiene su propio topic "crudo" (raw). Separarlos permite:
 * <ul>
 *   <li>Aislar la carga: una avalancha de lecturas LPR no retrasa los botones de panico.</li>
 *   <li>Definir retencion y particiones distintas segun el volumen de cada sensor.</li>
 *   <li>Evolucionar el esquema de un sensor sin afectar a los demas.</li>
 * </ul>
 */
public final class Topics {

    /** Eventos crudos de botones de panico (apps y postes fisicos). */
    public static final String RAW_PANIC = "sensor.panic.raw";
    /** Lecturas crudas de camaras LPR (lectura de placas). */
    public static final String RAW_LPR = "sensor.lpr.raw";
    /** Lecturas crudas de sensores de velocidad / movimiento. */
    public static final String RAW_SPEED = "sensor.speed.raw";
    /** Detecciones crudas de sensores acusticos. */
    public static final String RAW_ACOUSTIC = "sensor.acoustic.raw";
    /** Reportes ciudadanos crudos (app o punto fisico). */
    public static final String RAW_CITIZEN = "sensor.citizen.raw";

    /** Eventos validados, normalizados y anonimizados. Llave del mensaje = id de zona. */
    public static final String NORMALIZED = "events.normalized";
    /** Alertas generadas por el motor de correlacion. */
    public static final String ALERTS = "alerts";
    /** Cola de mensajes muertos: eventos que no pudieron procesarse tras los reintentos. */
    public static final String DLQ = "events.dlq";

    /**
     * Particiones de los topics de alto volumen. Es el maximo de consumidores en paralelo por grupo:
     * 12 permite, por ejemplo, 2 maquinas x 6 hilos o 4 maquinas x 3 hilos procesando a la vez.
     */
    public static final int PARTITIONS = 12;
    /** Particiones del topic de alertas (bajo volumen). */
    public static final int ALERT_PARTITIONS = 3;

    private Topics() {
    }
}
