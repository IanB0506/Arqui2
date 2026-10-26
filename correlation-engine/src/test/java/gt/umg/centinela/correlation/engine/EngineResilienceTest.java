package gt.umg.centinela.correlation.engine;

import gt.umg.centinela.common.geo.ZoneCatalog;
import gt.umg.centinela.common.model.AlertEvent;
import gt.umg.centinela.common.model.NormalizedEvent;
import gt.umg.centinela.common.model.SensorType;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Garantias ante caidas (ADR-006): una alerta nunca se pierde porque Kafka fallo al publicarla, y no
 * se clasifica un falso positivo mientras el motor se pone al dia (sin depender de los relojes de los nodos).
 */
class EngineResilienceTest {

    private static final Instant T0 = Instant.parse("2026-10-30T04:05:00Z");

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

    private static final AlertSink KAFKA_DOWN = alert -> {
        throw new IllegalStateException("No se pudo publicar la alerta: Kafka caido");
    };

    @Test
    void siKafkaFallaLaAlertaNoSePierdeYElReintentoEmiteLaMismaAlerta() {
        engine.onEvent(event(SensorType.PANIC, 4, "ROBO", null, "APP-1", 0));
        NormalizedEvent speed = event(SensorType.SPEED, 4, "EXCESO_VELOCIDAD", 85.0, "VEL-Z04-02", 15);
        assertThatThrownBy(() -> engine.onEvent(speed, KAFKA_DOWN)).isInstanceOf(IllegalStateException.class);
        // el consumidor reintenta el mismo evento
        List<AlertEvent> retry = engine.onEvent(speed);
        assertThat(retry).singleElement().satisfies(a -> assertThat(a.alertType()).isEqualTo("ROBO_EN_CURSO"));
        // y es la misma alerta que se habria emitido: el alert-service la guarda una sola vez
        assertThat(retry.get(0).alertId()).isNotNull();
        assertThat(engine.onEvent(speed)).isEmpty(); // ya registrada: no se repite
    }

    @Test
    void unFalsoPositivoNoSePierdeSiFallaSuPublicacion() {
        engine.onEvent(event(SensorType.PANIC, 10, "PANICO", null, "APP-2", 0));
        clock.advance(Duration.ofSeconds(91));
        assertThatThrownBy(() -> engine.sweepPending(KAFKA_DOWN)).isInstanceOf(IllegalStateException.class);
        clock.advance(Duration.ofSeconds(6));
        assertThat(engine.sweepPending()).extracting(AlertEvent::alertType).containsExactly("POSIBLE_FALSO_POSITIVO");
        clock.advance(Duration.ofSeconds(6));
        assertThat(engine.sweepPending()).isEmpty();
    }

    @Test
    void elIdDelFalsoPositivoEsDeterminista() {
        NormalizedEvent panic = event(SensorType.PANIC, 10, "PANICO", null, "APP-3", 0);
        engine.onEvent(panic);
        clock.advance(Duration.ofSeconds(91));
        List<AlertEvent> first = new ArrayList<>();
        engine.sweepPending(first::add);
        // el mismo ancla reclasificada (p. ej. tras un reintento) produce el mismo id
        state.addPending(first.get(0).ruleId().replace("/sin-corroborar", ""), panic, clock.instant());
        state.corroborated.clear();
        List<AlertEvent> again = engine.sweepPending();
        assertThat(again).singleElement().extracting(AlertEvent::alertId).isEqualTo(first.get(0).alertId());
    }

    @Test
    void mientrasSeProcesaAtrasoNoSeClasificanFalsosPositivos() {
        engine.onEvent(event(SensorType.PANIC, 10, "PANICO", null, "APP-4", 0));
        clock.advance(Duration.ofSeconds(91));
        // Kafka reporta 5000 mensajes en espera en la particion: el motor esta poniendose al dia
        engine.noteConsumerLag(5_000);
        assertThat(state.isBacklogged()).isTrue();
        assertThat(engine.sweepPending()).isEmpty();
        state.backlogged = false; // vencio la marca de atraso
        assertThat(engine.sweepPending()).extracting(AlertEvent::alertType).containsExactly("POSIBLE_FALSO_POSITIVO");
    }

    @Test
    void losEventosReenviadosDesdeUnSpoolPausanLaClasificacion() {
        engine.onEvent(event(SensorType.PANIC, 10, "PANICO", null, "APP-5", 0));
        clock.advance(Duration.ofSeconds(91));
        // tras una caida de Kafka llega telemetria guardada en el spool del simulador
        engine.onEvent(spooled(event(SensorType.SPEED, 3, "NORMAL", 40.0, "VEL-Z03-01", 30)));
        assertThat(state.isBacklogged()).isTrue();
        assertThat(engine.sweepPending()).isEmpty();
    }

    @Test
    void unRelojDesfasadoEntreNodosNoDetieneLaClasificacion() {
        engine.onEvent(event(SensorType.PANIC, 10, "PANICO", null, "APP-6", 0));
        clock.advance(Duration.ofSeconds(91));
        // la ingesta corre en otra PC con el reloj 60 s atrasado y la cola de Kafka esta al dia
        engine.onEvent(event(SensorType.LPR, 3, "LECTURA_PLACA", 50.0, "LPR-Z03-01", 31));
        engine.noteConsumerLag(3);
        assertThat(state.isBacklogged()).isFalse();
        assertThat(engine.sweepPending()).extracting(AlertEvent::alertType).containsExactly("POSIBLE_FALSO_POSITIVO");
    }

    @Test
    void trasRecibirParticionesSeEsperaAntesDeClasificar() {
        engine.onEvent(event(SensorType.PANIC, 10, "PANICO", null, "APP-7", 0));
        clock.advance(Duration.ofSeconds(91));
        engine.notePartitionsAssigned(); // llegan las particiones de una instancia caida: puede haber atraso detras
        assertThat(engine.sweepPending()).isEmpty();
        clock.advance(Duration.ofSeconds(41));
        assertThat(engine.sweepPending()).extracting(AlertEvent::alertType).containsExactly("POSIBLE_FALSO_POSITIVO");
    }

    @Test
    void mientrasUnaParticionNoLograProcesarNoSeClasificaYAlVolverSeEspera() {
        engine.onEvent(event(SensorType.PANIC, 10, "PANICO", null, "APP-9", 0));
        clock.advance(Duration.ofSeconds(91));
        // Redis no responde: el consumidor reintenta (cada vez con mas espera, hasta 30 s)
        engine.noteProcessingFailure("events.normalized-9");
        assertThat(engine.sweepPending()).isEmpty();
        clock.advance(Duration.ofSeconds(30));
        engine.noteProcessingFailure("events.normalized-9");
        clock.advance(Duration.ofSeconds(25));
        assertThat(engine.sweepPending()).isEmpty();
        // vuelve: procesa su atraso, que puede traer la corroboracion; se espera un poco mas
        engine.noteProcessingOk("events.normalized-9");
        assertThat(state.isBacklogged()).isTrue();
        state.backlogged = false;
        assertThat(engine.sweepPending()).isEmpty();
        clock.advance(Duration.ofSeconds(41));
        assertThat(engine.sweepPending()).extracting(AlertEvent::alertType).containsExactly("POSIBLE_FALSO_POSITIVO");
    }

    @Test
    void unaParticionQueDejoDeReintentarNoBloqueaParaSiempre() {
        engine.onEvent(event(SensorType.PANIC, 10, "PANICO", null, "APP-10", 0));
        clock.advance(Duration.ofSeconds(91));
        engine.noteProcessingFailure("events.normalized-3"); // p. ej. Kafka se la asigno a otra instancia
        clock.advance(Duration.ofSeconds(61));
        assertThat(engine.sweepPending()).extracting(AlertEvent::alertType).containsExactly("POSIBLE_FALSO_POSITIVO");
    }

    @Test
    void alArrancarSeEsperaAntesDeClasificar() {
        // un ancla que otra instancia dejo pendiente y vencio mientras esta arrancaba
        state.addPending("R1-ROBO", event(SensorType.PANIC, 10, "PANICO", null, "APP-8", 0), T0);
        assertThat(engine.sweepPending()).isEmpty();
        clock.advance(Duration.ofSeconds(41));
        assertThat(engine.sweepPending()).extracting(AlertEvent::alertType).containsExactly("POSIBLE_FALSO_POSITIVO");
    }

    @Test
    void unAbusoDeBotonNoPublicadoNoDejaMarcadoElDispositivo() {
        for (int i = 0; i < 5; i++) {
            engine.onEvent(event(SensorType.PANIC, 2, "PANICO", null, "BTN-Z02-01", i));
        }
        NormalizedEvent sixth = event(SensorType.PANIC, 2, "PANICO", null, "BTN-Z02-01", 6);
        assertThatThrownBy(() -> engine.onEvent(sixth, KAFKA_DOWN)).isInstanceOf(IllegalStateException.class);
        assertThat(state.isDeviceFlagged("BTN-Z02-01")).isFalse();
        assertThat(engine.onEvent(event(SensorType.PANIC, 2, "PANICO", null, "BTN-Z02-01", 7)))
                .extracting(AlertEvent::alertType).contains("DISPOSITIVO_SOSPECHOSO");
    }

    private static NormalizedEvent spooled(NormalizedEvent e) {
        return new NormalizedEvent(e.eventId(), e.sensorType(), e.sensorId(), e.zoneId(), e.zoneName(), e.latitude(),
                e.longitude(), e.eventTime(), e.ingestedAt(), e.subtype(), e.value(), e.confidence(), e.anomalous(),
                e.plateHash(), e.plateMasked(), e.sourceHash(), e.origin(), Map.of(NormalizedEvent.ATTR_SPOOLED, "true"));
    }

    private static NormalizedEvent event(SensorType type, int zone, String subtype, Double value, String sensorId,
                                         int secondsAfterT0) {
        var z = ZoneCatalog.byId(zone);
        Instant time = T0.plusSeconds(secondsAfterT0);
        boolean anomalous = !"LECTURA_PLACA".equals(subtype);
        return new NormalizedEvent(UUID.randomUUID(), type, sensorId, zone, z.name(), z.latitude(), z.longitude(), time,
                time.plusMillis(20), subtype, value, 1.0, anomalous, type == SensorType.LPR ? "hash-" + sensorId : null,
                type == SensorType.LPR ? "ABC***" : null, null, "TEST", Map.of());
    }
}
