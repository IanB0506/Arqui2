package gt.umg.centinela.simulator.generator;

import gt.umg.centinela.common.geo.Zone;
import gt.umg.centinela.common.geo.ZoneCatalog;
import gt.umg.centinela.common.model.raw.CitizenReportEvent;
import gt.umg.centinela.common.model.raw.LprCameraEvent;
import gt.umg.centinela.common.model.raw.PanicButtonEvent;
import gt.umg.centinela.simulator.config.SimulatorProperties;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Los puntos del mapa caen dentro de su zona: el geocercado los devuelve a la misma zona. */
class EventFactoryZonesTest {

    private final EventFactory factory = new EventFactory(new SimulatorProperties());

    @Test
    void losReportesCiudadanosSeResuelvenSiempreASuZona() {
        for (Zone zone : ZoneCatalog.all()) {
            for (int i = 0; i < 300; i++) {
                CitizenReportEvent e = (CitizenReportEvent) factory.randomCitizen(zone.id()).payload();
                assertThat(ZoneCatalog.nearest(e.location().latitude(), e.location().longitude()).id())
                        .as("reporte generado en %s", zone.name()).isEqualTo(zone.id());
            }
        }
    }

    @Test
    void panicosYCamarasSeDibujanDentroDeSuZona() {
        for (Zone zone : ZoneCatalog.all()) {
            for (int i = 0; i < 100; i++) {
                PanicButtonEvent p = (PanicButtonEvent) factory.randomPanic(zone.id()).payload();
                assertThat(ZoneCatalog.nearest(p.zone().latitude(), p.zone().longitude()).id()).isEqualTo(zone.id());
            }
            for (int camera = 1; camera <= 3; camera++) {
                LprCameraEvent l = (LprCameraEvent) factory.lpr(zone.id(), camera, "P123ABC", 60).payload();
                assertThat(ZoneCatalog.nearest(l.location().latitude(), l.location().longitude()).id()).isEqualTo(zone.id());
            }
        }
    }

    @Test
    void lasZonasEstanEnLaCiudadDeGuatemala() {
        for (Zone zone : ZoneCatalog.all()) {
            assertThat(zone.latitude()).isBetween(14.57, 14.67);
            assertThat(zone.longitude()).isBetween(-90.57, -90.48);
        }
    }
}
