package gt.umg.centinela.ingestion.pipeline;

import java.util.UUID;

/**
 * Deduplicacion por event_id con identidad de entrega. La marca guarda que mensaje la puso
 * (topic:particion:offset del mensaje crudo) y si ya se publico:
 * <ul>
 *   <li>una reentrega del MISMO mensaje (el proceso murio o la publicacion fallo) encuentra su marca
 *       pendiente y continua: no se pierde;</li>
 *   <li>una copia en otro offset (reenvio de un spool, reintento de la app) o la reentrega de algo ya
 *       publicado se descarta como duplicado.</li>
 * </ul>
 */
public interface DedupStore {

    enum Claim { NUEVO, REINTENTO_PROPIO, DUPLICADO, SIN_DEDUPLICACION }

    /** Pone la marca en "pendiente" (o la renueva si es de esta misma entrega). */
    Claim claim(UUID eventId, String deliveryToken);

    /** El evento se publico: la marca pasa a "publicada" y vive todo el TTL de deduplicacion. */
    void confirm(UUID eventId, String deliveryToken);

    /** La publicacion fallo: se borra la marca si es de esta entrega y sigue pendiente. */
    void release(UUID eventId, String deliveryToken);
}
