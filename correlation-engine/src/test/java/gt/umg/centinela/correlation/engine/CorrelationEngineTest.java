package gt.umg.centinela.correlation.engine;

import gt.umg.centinela.common.geo.ZoneCatalog;
import gt.umg.centinela.common.model.AlertEvent;
import gt.umg.centinela.common.model.AlertStatus;
import gt.umg.centinela.common.model.NormalizedEvent;
import gt.umg.centinela.common.model.SensorType;
import gt.umg.centinela.common.model.Severity;
import gt.umg.centinela.correlation.config.CorrelationProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.bind.PropertySourcesPlaceholdersResolver;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pruebas del modelo de correlacion usando las MISMAS reglas de application.yml.
 * Reproducen la tabla 2.4 del enunciado y los demas casos de uso.
 */
class CorrelationEngineTest {

    private static final Instant T0 = Instant.parse("2026-10-30T04:05:00Z"); // 22:05 hora de Guatemala

    private MutableClock clock;
    private InMemoryCorrelationState state;
    private CorrelationEngine engine;

    @BeforeEach
    void setUp() throws Exception {
        List<PropertySource<?>> sources = new YamlPropertySourceLoader()
                .load("application", new ClassPathResource("application.yml"));
        CorrelationProperties properties = new Binder(ConfigurationPropertySources.from(sources),
                new PropertySourcesPlaceholdersResolver(sources))
                .bind("correlation", CorrelationProperties.class).get();
        clock = new MutableClock(T0);
        state = new InMemoryCorrelationState();
        engine = new CorrelationEngine(properties, new InMemoryWindowStore(), state, () -> state.adjustments, clock);
    }

    @Test
    void table24Row1PanicPlusLprPlusSpeedIsRobberyInProgress() {
        assertThat(feed(event(SensorType.PANIC, 4, "ROBO", null, true, "APP-1", "user-1", 0))).isEmpty();
        List<AlertEvent> afterSpeed = feed(event(SensorType.SPEED, 4, "EXCESO_VELOCIDAD", 85.0, true, "VEL-Z04-02", null, 15));
        assertThat(afterSpeed).singleElement().satisfies(a -> {
            assertThat(a.alertType()).isEqualTo("ROBO_EN_CURSO");
            assertThat(a.severity()).isEqualTo(Severity.HIGH);
            assertThat(a.confidence()).isEqualTo(0.70);
            assertThat(a.status()).isEqualTo(AlertStatus.NEW);
        });
        List<AlertEvent> afterLpr = feed(lpr(4, "hash-xyz123", "XYZ***", 87.0, 18));
        assertThat(afterLpr).singleElement().satisfies(a -> {
            assertThat(a.alertId()).isEqualTo(afterSpeed.get(0).alertId());
            assertThat(a.severity()).isEqualTo(Severity.CRITICAL);
            assertThat(a.status()).isEqualTo(AlertStatus.ESCALATED);
            assertThat(a.revision()).isEqualTo(2);
            assertThat(a.plateMasked()).isEqualTo("XYZ***");
            assertThat(a.evidence()).hasSize(3);
        });
        // al vencer el plazo, el panico ya esta corroborado: no debe aparecer como falso positivo
        clock.advance(Duration.ofMinutes(3));
        assertThat(engine.sweepPending()).isEmpty();
    }

    @Test
    void secondWitnessUpdatesTheOpenIncidentWithoutDuplicatingIt() {
        feed(event(SensorType.PANIC, 6, "ROBO", null, true, "APP-10", "user-10", 0));
        AlertEvent first = feed(event(SensorType.SPEED, 6, "EXCESO_VELOCIDAD", 90.0, true, "VEL-Z06-01", null, 10)).get(0);
        List<AlertEvent> update = feed(event(SensorType.PANIC, 6, "ASALTO", null, true, "BTN-Z06-02", null, 30));
        assertThat(update).singleElement().satisfies(a -> {
            assertThat(a.alertId()).isEqualTo(first.alertId());
            assertThat(a.status()).isEqualTo(AlertStatus.UPDATED);
            assertThat(a.revision()).isEqualTo(2);
            assertThat(a.severity()).isEqualTo(Severity.HIGH);
        });
        // una evidencia mas para el mismo ancla, sin subir de severidad, no genera otra revision
        assertThat(feed(event(SensorType.SPEED, 6, "EXCESO_VELOCIDAD", 85.0, true, "VEL-Z06-03", null, 40))).isEmpty();
    }

    @Test
    void secondRobberyMinutesLaterUpdatesTheOpenIncidentRightAway() {
        feed(event(SensorType.PANIC, 3, "ROBO", null, true, "APP-20", "user-20", 0));
        feed(event(SensorType.SPEED, 3, "EXCESO_VELOCIDAD", 85.0, true, "VEL-Z03-01", null, 15));
        AlertEvent first = feed(lpr(3, "hash-abc", "ABC***", 87.0, 18)).get(0);
        assertThat(first.severity()).isEqualTo(Severity.CRITICAL);
        // tres minutos despues otra persona presiona panico y llega su propia evidencia. La coincidencia de
        // mayor puntaje sigue siendo la del primer panico, que ya esta en el incidente y no aporta nada.
        clock.advance(Duration.ofMinutes(3));
        assertThat(feed(event(SensorType.PANIC, 3, "ROBO", null, true, "APP-21", "user-21", 180))).isEmpty();
        List<AlertEvent> update = feed(event(SensorType.SPEED, 3, "EXCESO_VELOCIDAD", 92.0, true, "VEL-Z03-02", null, 195));
        // la revision sale con la evidencia del segundo panico, no 90 s despues al vencer su plazo
        assertThat(update).singleElement().satisfies(a -> {
            assertThat(a.alertId()).isEqualTo(first.alertId());
            assertThat(a.status()).isEqualTo(AlertStatus.UPDATED);
            assertThat(a.revision()).isEqualTo(3);
            assertThat(a.severity()).isEqualTo(Severity.CRITICAL);
        });
        clock.advance(Duration.ofMinutes(2));
        assertThat(engine.sweepPending()).isEmpty();
    }

    @Test
    void table24Row2LonePanicBecomesPossibleFalsePositive() {
        assertThat(feed(event(SensorType.PANIC, 10, "PANICO", null, true, "APP-2", "user-2", 0))).isEmpty();
        clock.advance(Duration.ofSeconds(60));
        assertThat(engine.sweepPending()).isEmpty();
        clock.advance(Duration.ofSeconds(31));
        assertThat(engine.sweepPending()).singleElement().satisfies(a -> {
            assertThat(a.alertType()).isEqualTo("POSIBLE_FALSO_POSITIVO");
            assertThat(a.severity()).isEqualTo(Severity.LOW);
            assertThat(a.status()).isEqualTo(AlertStatus.AUTO_CLASSIFIED);
            assertThat(a.zoneId()).isEqualTo(10);
        });
    }

    @Test
    void evidenceInAnotherZoneDoesNotCorroborate() {
        feed(event(SensorType.PANIC, 4, "ROBO", null, true, "APP-3", "user-3", 0));
        assertThat(feed(event(SensorType.SPEED, 5, "EXCESO_VELOCIDAD", 95.0, true, "VEL-Z05-01", null, 10))).isEmpty();
    }

    @Test
    void sameCitizenCannotCorroborateThemself() {
        feed(event(SensorType.PANIC, 3, "ROBO", null, true, "APP-4", "same-user", 0));
        feed(event(SensorType.CITIZEN_REPORT, 3, "ROBO", null, true, "REPORTE-APP", "same-user", 5));
        feed(event(SensorType.CITIZEN_REPORT, 3, "ROBO", null, true, "REPORTE-APP", "same-user", 8));
        clock.advance(Duration.ofMinutes(2));
        assertThat(engine.sweepPending()).extracting(AlertEvent::alertType).containsExactly("POSIBLE_FALSO_POSITIVO");
    }

    @Test
    void escapingVehicleIsTrackedAcrossZones() {
        feed(event(SensorType.PANIC, 4, "ROBO", null, true, "APP-5", "user-5", 0));
        feed(event(SensorType.SPEED, 4, "EXCESO_VELOCIDAD", 85.0, true, "VEL-Z04-02", null, 15));
        feed(lpr(4, "hash-xyz123", "XYZ***", 87.0, 18));
        clock.advance(Duration.ofSeconds(90));
        List<AlertEvent> sighting = feed(lpr(8, "hash-xyz123", "XYZ***", 93.0, 90));
        assertThat(sighting).singleElement().satisfies(a -> {
            assertThat(a.alertType()).isEqualTo("VEHICULO_EN_FUGA");
            assertThat(a.zoneId()).isEqualTo(8);
            assertThat(a.parentAlertId()).isNotNull();
            assertThat(a.severity()).isEqualTo(Severity.CRITICAL);
        });
        assertThat(feed(lpr(8, "hash-xyz123", "XYZ***", 60.0, 100))).isEmpty(); // misma zona: no se repite
    }

    @Test
    void samePlateInANewIncidentIsTrackedAgain() {
        escapingVehicleIsTrackedAcrossZones();
        clock.advance(Duration.ofMinutes(12)); // vencio el cooldown del primer incidente
        state.saveIncident("ROBO_EN_CURSO:4", null, Duration.ZERO);
        int t = 12 * 60;
        feed(event(SensorType.PANIC, 4, "ASALTO", null, true, "APP-9", "user-9", t));
        feed(event(SensorType.SPEED, 4, "EXCESO_VELOCIDAD", 88.0, true, "VEL-Z04-01", null, t + 10));
        assertThat(feed(lpr(4, "hash-xyz123", "XYZ***", 91.0, t + 12))).singleElement()
                .extracting(AlertEvent::alertType).isEqualTo("ROBO_EN_CURSO");
        clock.advance(Duration.ofMinutes(1));
        assertThat(feed(lpr(8, "hash-xyz123", "XYZ***", 95.0, t + 80))).singleElement()
                .extracting(AlertEvent::alertType).isEqualTo("VEHICULO_EN_FUGA");
    }

    @Test
    void abusiveDeviceIsFlaggedAndItsPanicsIgnored() {
        List<AlertEvent> alerts = new ArrayList<>();
        for (int i = 0; i < 7; i++) {
            alerts.addAll(feed(event(SensorType.PANIC, 1, "PANICO", null, true, "APP-ABUSO", "abuser", i * 5)));
        }
        assertThat(alerts).extracting(AlertEvent::alertType).containsExactly("DISPOSITIVO_SOSPECHOSO");
        clock.advance(Duration.ofMinutes(3));
        assertThat(engine.sweepPending()).isEmpty();
    }

    @Test
    void gunshotsNeedMoreThanOneSignal() {
        assertThat(feed(acoustic(2, "ACU-Z02-01", "DISPARO", 0.93, 0))).isEmpty();
        assertThat(feed(acoustic(2, "ACU-Z02-02", "DISPARO", 0.90, 1))).isEmpty();
        List<AlertEvent> alerts = feed(event(SensorType.PANIC, 2, "ASALTO", null, true, "BTN-Z02-03", null, 10));
        assertThat(alerts).singleElement().satisfies(a -> {
            assertThat(a.alertType()).isEqualTo("DISPAROS_CONFIRMADOS");
            assertThat(a.severity()).isEqualTo(Severity.CRITICAL);
        });
        clock.advance(Duration.ofMinutes(3));
        assertThat(engine.sweepPending()).isEmpty(); // disparos y panico quedaron corroborados
    }

    @Test
    void lonelyFireworkIsAFalsePositive() {
        feed(acoustic(9, "ACU-Z09-01", "DISPARO", 0.75, 0));
        clock.advance(Duration.ofMinutes(2));
        assertThat(engine.sweepPending()).extracting(AlertEvent::alertType).containsExactly("POSIBLE_FALSO_POSITIVO");
    }

    @Test
    void accidentDetectedBySensorsEscalatesWithCitizenReport() {
        assertThat(feed(acoustic(7, "ACU-Z07-01", "CHOQUE_VEHICULAR", 0.88, 0))).isEmpty();
        assertThat(feed(event(SensorType.SPEED, 7, "EXCESO_VELOCIDAD", 97.0, true, "VEL-Z07-03", null, 4)))
                .singleElement().satisfies(a -> {
                    assertThat(a.alertType()).isEqualTo("ACCIDENTE_VIAL");
                    assertThat(a.severity()).isEqualTo(Severity.HIGH);
                });
        assertThat(feed(event(SensorType.CITIZEN_REPORT, 7, "ACCIDENTE", null, true, "REPORTE-APP", "c-1", 20)))
                .singleElement().satisfies(a -> {
                    assertThat(a.severity()).isEqualTo(Severity.CRITICAL);
                    assertThat(a.status()).isEqualTo(AlertStatus.ESCALATED);
                });
    }

    @Test
    void fireEscalatesWithExplosion() {
        assertThat(feed(event(SensorType.CITIZEN_REPORT, 11, "INCENDIO", null, true, "REPORTE-APP", "c-2", 0))).isEmpty();
        assertThat(feed(event(SensorType.PANIC, 11, "INCENDIO", null, true, "BTN-Z11-01", null, 8)))
                .singleElement().extracting(AlertEvent::severity).isEqualTo(Severity.HIGH);
        assertThat(feed(acoustic(11, "ACU-Z11-01", "EXPLOSION", 0.81, 15)))
                .singleElement().extracting(AlertEvent::severity).isEqualTo(Severity.CRITICAL);
    }

    @Test
    void watchlistedPlateRaisesStolenVehicleAlert() {
        state.watchlist.add("hash-robado");
        assertThat(feed(lpr(12, "hash-robado", "P51***", 55.0, 0))).singleElement().satisfies(a -> {
            assertThat(a.alertType()).isEqualTo("VEHICULO_REPORTADO");
            assertThat(a.severity()).isEqualTo(Severity.HIGH);
        });
    }

    @Test
    void anotherPlateSpeedingDoesNotCorroborateTheStolenVehicle() {
        state.watchlist.add("hash-robado");
        feed(lpr(11, "hash-robado", "P51***", 68.0, 0));
        // una camara de la misma zona capta OTRO vehiculo a alta velocidad: no corrobora al robado
        assertThat(feed(lpr(11, "hash-otro", "XYZ***", 95.0, 26, "LPR-Z11-02"))).isEmpty();
        // un sensor de velocidad no lee placas: puede ser el mismo vehiculo y si suma
        assertThat(feed(event(SensorType.SPEED, 11, "EXCESO_VELOCIDAD", 96.0, true, "VEL-Z11-01", null, 30)))
                .singleElement().satisfies(a -> {
                    assertThat(a.alertType()).isEqualTo("VEHICULO_REPORTADO");
                    assertThat(a.severity()).isEqualTo(Severity.CRITICAL);
                    assertThat(a.evidence()).extracting(e -> e.sensorId()).doesNotContain("LPR-Z11-02");
                });
    }

    @Test
    void medicalEmergenciesAreSeparateIncidents() {
        assertThat(feed(event(SensorType.PANIC, 5, "EMERGENCIA_MEDICA", null, true, "APP-6", "u-6", 0)))
                .extracting(AlertEvent::alertType).containsExactly("EMERGENCIA_MEDICA");
        assertThat(feed(event(SensorType.PANIC, 5, "EMERGENCIA_MEDICA", null, true, "APP-7", "u-7", 30)))
                .extracting(AlertEvent::alertType).containsExactly("EMERGENCIA_MEDICA");
    }

    @Test
    void adaptiveThresholdRequiresMoreEvidenceInNoisyZone() {
        state.adjustments.put("R1-ROBO:4", 0.10); // la zona 4 tuvo muchos falsos positivos
        feed(event(SensorType.PANIC, 4, "ROBO", null, true, "APP-8", "u-8", 0));
        assertThat(feed(event(SensorType.SPEED, 4, "EXCESO_VELOCIDAD", 85.0, true, "VEL-Z04-02", null, 15))).isEmpty();
        assertThat(feed(lpr(4, "hash-abc", "ABC***", 90.0, 18))).singleElement()
                .extracting(AlertEvent::severity).isEqualTo(Severity.CRITICAL);
    }

    // ------------------------------------------------------------------ utilidades

    private List<AlertEvent> feed(NormalizedEvent event) {
        return engine.onEvent(event);
    }

    private static NormalizedEvent event(SensorType type, int zone, String subtype, Double value, boolean anomalous,
                                         String sensorId, String source, int secondsAfterT0) {
        var z = ZoneCatalog.byId(zone);
        Instant time = T0.plusSeconds(secondsAfterT0);
        return new NormalizedEvent(UUID.randomUUID(), type, sensorId, zone, z.name(), z.latitude(), z.longitude(), time,
                time.plusMillis(20), subtype, value, 1.0, anomalous, null, null, source, "TEST", Map.of());
    }

    private static NormalizedEvent lpr(int zone, String plateHash, String masked, double speed, int secondsAfterT0) {
        return lpr(zone, plateHash, masked, speed, secondsAfterT0, "LPR-Z%02d-01".formatted(zone));
    }

    private static NormalizedEvent lpr(int zone, String plateHash, String masked, double speed, int secondsAfterT0,
                                       String sensorId) {
        var z = ZoneCatalog.byId(zone);
        Instant time = T0.plusSeconds(secondsAfterT0);
        return new NormalizedEvent(UUID.randomUUID(), SensorType.LPR, sensorId, zone, z.name(),
                z.latitude(), z.longitude(), time, time.plusMillis(20), speed > 78 ? "EXCESO_VELOCIDAD" : "LECTURA_PLACA",
                speed, 1.0, speed > 78, plateHash, masked, null, "SENSOR", Map.of());
    }

    private static NormalizedEvent acoustic(int zone, String sensorId, String sound, double probability, int secondsAfterT0) {
        var z = ZoneCatalog.byId(zone);
        Instant time = T0.plusSeconds(secondsAfterT0);
        return new NormalizedEvent(UUID.randomUUID(), SensorType.ACOUSTIC, sensorId, zone, z.name(), z.latitude(),
                z.longitude(), time, time.plusMillis(20), sound, 120.0, probability, probability >= 0.5, null, null,
                null, "SENSOR", Map.of());
    }
}
