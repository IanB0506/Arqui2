package gt.umg.centinela.common;

import gt.umg.centinela.common.geo.ZoneCatalog;
import gt.umg.centinela.common.privacy.PlatePseudonymizer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CommonTest {

    @Test
    void resolvesZoneNamesInSeveralFormats() {
        assertThat(ZoneCatalog.byName("Zona 4")).get().extracting(z -> z.id()).isEqualTo(4);
        assertThat(ZoneCatalog.byName("zona 10")).get().extracting(z -> z.id()).isEqualTo(10);
        assertThat(ZoneCatalog.byName("Z7")).get().extracting(z -> z.id()).isEqualTo(7);
        assertThat(ZoneCatalog.byName("Zona 99")).isEmpty();
        assertThat(ZoneCatalog.byName("Centro")).isEmpty();
    }

    @Test
    void nearestZoneOfACentroidIsThatZone() {
        ZoneCatalog.all().forEach(zone ->
                assertThat(ZoneCatalog.nearest(zone.latitude() + 0.001, zone.longitude() - 0.001)).isEqualTo(zone));
    }

    @Test
    void cornerZoneHasTwoNeighbors() {
        assertThat(ZoneCatalog.neighbors(1)).extracting(z -> z.id()).containsExactlyInAnyOrder(2, 5);
        assertThat(ZoneCatalog.neighbors(6)).hasSize(4);
    }

    @Test
    void pseudonymIsStableAndDependsOnSecret() {
        PlatePseudonymizer a = new PlatePseudonymizer("secreto-de-prueba-123");
        PlatePseudonymizer b = new PlatePseudonymizer("otro-secreto-distinto-456");
        assertThat(a.pseudonymizePlate("XYZ123")).isEqualTo(a.pseudonymizePlate("xyz-123"));
        assertThat(a.pseudonymizePlate("XYZ123")).isNotEqualTo(b.pseudonymizePlate("XYZ123"));
        assertThat(a.pseudonymizePlate("XYZ123")).hasSize(32).doesNotContain("XYZ");
        assertThat(PlatePseudonymizer.maskPlate("P-123BCD")).isEqualTo("P12***");
    }
}
