package gt.umg.centinela.common.model;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Alerta producida por el motor de correlacion y publicada en el topic {@code alerts}.
 *
 * <p>Un mismo incidente puede publicarse varias veces con el mismo {@code alertId} y una
 * {@code revision} mayor cuando llega nueva evidencia (escalamiento). El consumidor hace upsert.
 *
 * @param alertId          identificador de la alerta / incidente
 * @param revision         numero de version del incidente (1, 2, 3...)
 * @param ruleId           regla de correlacion que la produjo
 * @param alertType        tipo de alerta: ROBO_EN_CURSO, ACCIDENTE_VIAL, POSIBLE_FALSO_POSITIVO...
 * @param severity         severidad calculada a partir del puntaje
 * @param status           estado inicial (NEW, ESCALATED, AUTO_CLASSIFIED)
 * @param zoneId           zona del incidente
 * @param zoneName         nombre de la zona
 * @param latitude         latitud aproximada del incidente
 * @param longitude        longitud aproximada del incidente
 * @param confidence       puntaje de confianza 0..1 de la inferencia
 * @param title            titulo corto para el operador
 * @param description      explicacion de por que se genero (explicabilidad)
 * @param createdAt        momento en que el motor genero esta revision
 * @param firstEventTime   hora del primer evento de la evidencia
 * @param triggerEventTime hora del evento que completo la correlacion (base para medir el SLO)
 * @param evidence         eventos que respaldan la alerta
 * @param plateMasked      placa sospechosa enmascarada, si existe
 * @param plateHash        seudonimo de la placa sospechosa, si existe
 * @param parentAlertId    alerta original cuando esta es un seguimiento (vehiculo en fuga)
 */
public record AlertEvent(
        UUID alertId,
        int revision,
        String ruleId,
        String alertType,
        Severity severity,
        AlertStatus status,
        int zoneId,
        String zoneName,
        double latitude,
        double longitude,
        double confidence,
        String title,
        String description,
        Instant createdAt,
        Instant firstEventTime,
        Instant triggerEventTime,
        List<Evidence> evidence,
        String plateMasked,
        String plateHash,
        UUID parentAlertId) {
}
