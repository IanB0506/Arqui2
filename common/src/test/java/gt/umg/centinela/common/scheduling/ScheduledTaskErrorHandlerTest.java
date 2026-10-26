package gt.umg.centinela.common.scheduling;

import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;

class ScheduledTaskErrorHandlerTest {

    @Test
    void elNombreDeLaTareaEsElPrimerMetodoPropioSinProxies() {
        RuntimeException e = new DataAccessResourceFailureException("Redis caido");
        e.setStackTrace(new StackTraceElement[]{
                new StackTraceElement("io.lettuce.core.RedisClient", "connect", null, 1),
                new StackTraceElement("gt.umg.centinela.alerts.jobs.WatchlistSyncJob$$SpringCGLIB$$0", "checkSentinel", null, 1),
                new StackTraceElement("gt.umg.centinela.alerts.jobs.WatchlistSyncJob", "checkSentinel", null, 2)});
        assertThat(ScheduledTaskErrorHandler.taskName(e)).isEqualTo("WatchlistSyncJob.checkSentinel");
    }

    @Test
    void lasFallasTransitoriasSeResumenUnaVezPorMinutoYPorTarea() {
        FakeClock clock = new FakeClock();
        ScheduledTaskErrorHandler handler = new ScheduledTaskErrorHandler(clock, Duration.ofMinutes(1));
        RuntimeException e = new DataAccessResourceFailureException("base caida");
        e.setStackTrace(new StackTraceElement[]{new StackTraceElement("gt.umg.centinela.alerts.jobs.SloMonitorJob", "measure", null, 1)});

        handler.handleError(e);                       // se registra
        assertThat(handler.pendingRepeats("SloMonitorJob.measure")).isZero();
        handler.handleError(e);                       // dentro del minuto: solo se cuenta
        handler.handleError(e);
        assertThat(handler.pendingRepeats("SloMonitorJob.measure")).isEqualTo(2);
        clock.now = clock.now.plusSeconds(61);
        handler.handleError(e);                       // pasado el minuto: se registra con el conteo
        assertThat(handler.pendingRepeats("SloMonitorJob.measure")).isZero();
    }

    @Test
    void unDefectoNoSeSilencia() {
        ScheduledTaskErrorHandler handler = new ScheduledTaskErrorHandler(new FakeClock(), Duration.ofMinutes(1));
        handler.handleError(new IllegalStateException("defecto"));
        handler.handleError(new IllegalStateException("defecto"));
        assertThat(handler.pendingRepeats("(desconocida)")).isZero(); // no pasa por el resumen
    }

    private static final class FakeClock extends Clock {
        private Instant now = Instant.parse("2026-10-15T12:00:00Z");

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }
}
