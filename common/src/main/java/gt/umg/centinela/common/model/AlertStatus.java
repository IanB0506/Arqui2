package gt.umg.centinela.common.model;

/**
 * Estado de una alerta.
 * <ul>
 *   <li>NEW: emitida por primera vez por el motor de correlacion.</li>
 *   <li>ESCALATED: el mismo incidente recibio nueva evidencia y subio de severidad.</li>
 *   <li>UPDATED: se sumo un nuevo testigo/ancla al incidente sin cambiar su severidad.</li>
 *   <li>AUTO_CLASSIFIED: clasificada automaticamente (p. ej. posible falso positivo).</li>
 *   <li>CONFIRMED / FALSE_POSITIVE: validada por un operador humano.</li>
 *   <li>EXPIRED: nadie la atendio en el tiempo maximo.</li>
 * </ul>
 */
public enum AlertStatus {
    NEW, ESCALATED, UPDATED, AUTO_CLASSIFIED, CONFIRMED, FALSE_POSITIVE, EXPIRED
}
