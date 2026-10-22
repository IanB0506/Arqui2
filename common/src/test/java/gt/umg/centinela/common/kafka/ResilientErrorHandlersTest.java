package gt.umg.centinela.common.kafka;

import org.junit.jupiter.api.Test;
import org.springframework.util.backoff.BackOffExecution;

import static org.assertj.core.api.Assertions.assertThat;

class ResilientErrorHandlersTest {

    private final ResilientErrorHandlers.Settings settings = ResilientErrorHandlers.Settings.defaults("prueba");

    @Test
    void losTransitoriosSeReintentanSinLimiteConEsperaAcotada() {
        BackOffExecution exec = ResilientErrorHandlers.transientBackOff(settings).start();
        long last = 0;
        for (int i = 0; i < 1000; i++) {
            last = exec.nextBackOff();
            assertThat(last).isNotEqualTo(BackOffExecution.STOP);
        }
        assertThat(last).isEqualTo(30_000);
    }

    @Test
    void losDesconocidosSeRindenTrasPocosIntentos() {
        BackOffExecution exec = ResilientErrorHandlers.unknownBackOff(settings).start();
        int attempts = 0;
        while (exec.nextBackOff() != BackOffExecution.STOP) {
            attempts++;
        }
        assertThat(attempts).isEqualTo(4);
    }
}
