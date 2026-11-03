package gt.umg.centinela.ingestion.pipeline;

import java.util.UUID;

public interface DedupStore {
    enum Claim { NUEVO, REINTENTO_PROPIO, DUPLICADO, SIN_DEDUPLICACION }

    Claim claim(UUID eventId, String deliveryToken);

    void confirm(UUID eventId, String deliveryToken);

    void release(UUID eventId, String deliveryToken);
}
