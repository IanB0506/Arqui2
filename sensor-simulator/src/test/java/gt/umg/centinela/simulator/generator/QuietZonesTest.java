package gt.umg.centinela.simulator.generator;

import gt.umg.centinela.common.model.SensorType;
import gt.umg.centinela.common.model.raw.AcousticSensorEvent;
import gt.umg.centinela.common.model.raw.LprCameraEvent;
import gt.umg.centinela.common.model.raw.SoundType;
import gt.umg.centinela.common.model.raw.SpeedSensorEvent;
import gt.umg.centinela.simulator.config.SimulatorProperties;
import gt.umg.centinela.simulator.publish.EventPublisher;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/** En una zona con un escenario en curso el trafico de fondo no mete ruido que pueda corroborarlo. */
class QuietZonesTest {

    @Test
    void enUnaZonaAisladaElFondoSoloEmiteLecturasNormales() {
        List<SimulatedEvent> events = run(true);
        assertThat(events).isNotEmpty();
        assertThat(events).extracting(SimulatedEvent::type)
                .doesNotContain(SensorType.PANIC, SensorType.CITIZEN_REPORT)
                .contains(SensorType.LPR, SensorType.SPEED, SensorType.ACOUSTIC);
        for (SimulatedEvent e : events) {
            switch (e.payload()) {
                case LprCameraEvent lpr -> assertThat(lpr.estimatedSpeed()).isLessThan(78); // exceso desde 78 km/h
                case SpeedSensorEvent speed -> {
                    assertThat(speed.speed()).isLessThanOrEqualTo(speed.speedLimit() * 1.25);
                    assertThat(speed.wrongWay()).isFalse();
                }
                case AcousticSensorEvent sound -> assertThat(sound.soundType()).isEqualTo(SoundType.RUIDO_AMBIENTAL);
                default -> throw new AssertionError("evento inesperado " + e.type());
            }
        }
    }

    @Test
    void sinAislarLasAnomaliasDelFondoSiguenSaliendo() {
        List<SimulatedEvent> events = run(false);
        assertThat(events).extracting(SimulatedEvent::type).contains(SensorType.PANIC, SensorType.CITIZEN_REPORT);
        assertThat(events).filteredOn(e -> e.payload() instanceof LprCameraEvent)
                .allSatisfy(e -> assertThat(((LprCameraEvent) e.payload()).estimatedSpeed()).isGreaterThanOrEqualTo(90));
    }

    @Test
    void elAislamientoVenceSolo() {
        MutableClock clock = new MutableClock(Instant.parse("2026-10-31T15:00:00Z"));
        QuietZones quiet = new QuietZones(clock);
        quiet.reserve(List.of(4, 10), clock.instant().plusSeconds(60));
        quiet.reserve(List.of(10), clock.instant().plusSeconds(30)); // no acorta un aislamiento mas largo
        clock.advance(Duration.ofSeconds(45));
        assertThat(quiet.isQuiet(10)).isTrue();
        assertThat(quiet.active()).containsOnlyKeys(4, 10);
        clock.advance(Duration.ofSeconds(16));
        assertThat(quiet.isQuiet(4)).isFalse();
        assertThat(quiet.active()).isEmpty();
    }

    /** 1 s de trafico de fondo en la Zona 10, con todas las lecturas anomalas si no estuviera aislada. */
    private static List<SimulatedEvent> run(boolean isolated) {
        SimulatorProperties properties = new SimulatorProperties();
        properties.setZones(List.of(10));
        Map<SensorType, Double> rates = new EnumMap<>(SensorType.class);
        for (SensorType type : SensorType.values()) {
            rates.put(type, 50.0);
        }
        properties.setRatesPerZone(rates);
        properties.setLprAnomalyProbability(1.0);
        properties.setSpeedAnomalyProbability(1.0);
        QuietZones quiet = new QuietZones();
        if (isolated) {
            quiet.reserve(List.of(10), Instant.now().plusSeconds(60));
        }
        EventPublisher publisher = mock(EventPublisher.class);
        BaselineGenerator baseline = new BaselineGenerator(properties, new EventFactory(properties), publisher, quiet);
        baseline.setEnabled(true);
        for (int i = 0; i < 10; i++) {
            baseline.tick();
        }
        ArgumentCaptor<SimulatedEvent> captor = ArgumentCaptor.forClass(SimulatedEvent.class);
        verify(publisher, atLeastOnce()).publish(captor.capture());
        return captor.getAllValues();
    }

    private static final class MutableClock extends Clock {
        private Instant now;

        MutableClock(Instant now) {
            this.now = now;
        }

        void advance(Duration duration) {
            now = now.plus(duration);
        }

        @Override
        public java.time.ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }
}
