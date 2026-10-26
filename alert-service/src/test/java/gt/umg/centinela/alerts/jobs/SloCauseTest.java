package gt.umg.centinela.alerts.jobs;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Causa de un incumplimiento del SLO segun el atraso de Kafka (ADR-008). */
class SloCauseTest {

    private static final long UMBRAL = 1_000;

    @Test
    void atrasoBajoEsRecuperacion() {
        assertThat(SloMonitorJob.classifyLag(0, 0L, UMBRAL)).isEqualTo(SloMonitorJob.RECOVERY);
        assertThat(SloMonitorJob.classifyLag(999, 5_000L, UMBRAL)).isEqualTo(SloMonitorJob.RECOVERY);
    }

    @Test
    void atrasoAltoTrasUnHuecoEnLasMedicionesEsLaDescargaDeUnaCaida() {
        // Caso real: al volver el nodo A, el spool del simulador se descargo de golpe (atraso 1 030)
        assertThat(SloMonitorJob.classifyLag(1_030, null, UMBRAL)).isEqualTo(SloMonitorJob.RECOVERY);
    }

    @Test
    void unPrimerAtrasoAltoSeObservaAntesDeDegradar() {
        assertThat(SloMonitorJob.classifyLag(1_500, 10L, UMBRAL)).isEqualTo(SloMonitorJob.WATCHING);
    }

    @Test
    void atrasoAltoSostenidoQueNoBajaEsSobrecarga() {
        assertThat(SloMonitorJob.classifyLag(5_000, 1_500L, UMBRAL)).isEqualTo(SloMonitorJob.OVERLOAD);
        assertThat(SloMonitorJob.classifyLag(4_700, 5_000L, UMBRAL)).isEqualTo(SloMonitorJob.OVERLOAD); // -6 %: no baja
    }

    @Test
    void atrasoAltoQueBajaEsRecuperacion() {
        assertThat(SloMonitorJob.classifyLag(2_000, 6_000L, UMBRAL)).isEqualTo(SloMonitorJob.RECOVERY);
    }
}
