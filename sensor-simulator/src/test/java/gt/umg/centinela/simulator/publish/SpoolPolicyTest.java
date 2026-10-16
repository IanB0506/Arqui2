package gt.umg.centinela.simulator.publish;

import gt.umg.centinela.common.model.raw.PanicAlertType;
import gt.umg.centinela.common.model.raw.SoundType;
import gt.umg.centinela.simulator.config.SimulatorProperties;
import gt.umg.centinela.simulator.generator.EventFactory;
import org.junit.jupiter.api.Test;

import static gt.umg.centinela.simulator.publish.SpoolPolicy.Decision.CRITICAL;
import static gt.umg.centinela.simulator.publish.SpoolPolicy.Decision.DISCARD;
import static gt.umg.centinela.simulator.publish.SpoolPolicy.Decision.TELEMETRY;
import static org.assertj.core.api.Assertions.assertThat;

class SpoolPolicyTest {

    private final SimulatorProperties properties = new SimulatorProperties();
    private final EventFactory factory = new EventFactory(properties);
    private final SpoolPolicy policy = new SpoolPolicy(properties);

    @Test
    void panicoYDisparoSonCriticos() {
        assertThat(policy.classify(factory.panic(4, PanicAlertType.ROBO, "dev-1"))).isEqualTo(CRITICAL);
        assertThat(policy.classify(factory.acoustic(4, 1, SoundType.DISPARO, 128, 0.9))).isEqualTo(CRITICAL);
        assertThat(policy.classify(factory.acoustic(4, 1, SoundType.DISPARO, 90, 0.3))).isEqualTo(DISCARD);
    }

    @Test
    void laTelemetriaUtilSeGuardaYLaNormalSeDescarta() {
        assertThat(policy.classify(factory.speed(4, 1, 85, false))).isEqualTo(TELEMETRY);   // limite 60
        assertThat(policy.classify(factory.speed(4, 1, 50, false))).isEqualTo(DISCARD);
        assertThat(policy.classify(factory.speed(4, 1, 40, true))).isEqualTo(TELEMETRY);    // contravia
        assertThat(policy.classify(factory.lpr(4, 1, "XYZ123", 50))).isEqualTo(TELEMETRY);
        assertThat(policy.classify(factory.acoustic(4, 1, SoundType.VIDRIOS_ROTOS, 100, 0.7))).isEqualTo(TELEMETRY);
        assertThat(policy.classify(factory.acoustic(4, 1, SoundType.RUIDO_AMBIENTAL, 70, 0.9))).isEqualTo(DISCARD);
    }

    @Test
    void sinTelemetriaHabilitadaSoloSeGuardanLosCriticos() {
        properties.getSpool().setTelemetryEnabled(false);
        assertThat(policy.classify(factory.lpr(4, 1, "XYZ123", 90))).isEqualTo(DISCARD);
        assertThat(policy.classify(factory.panic(4, PanicAlertType.ROBO, "dev-1"))).isEqualTo(CRITICAL);
    }
}
