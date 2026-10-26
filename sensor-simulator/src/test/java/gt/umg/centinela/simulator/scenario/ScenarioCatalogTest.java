package gt.umg.centinela.simulator.scenario;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ScenarioCatalogTest {

    @Test
    void fleeRouteGoesSouthWithoutRepeatingZones() {
        assertThat(ScenarioCatalog.fleeRoute(4, 2)).containsExactly(8, 12);
        assertThat(ScenarioCatalog.fleeRoute(12, 1)).containsExactly(11);
        assertThat(ScenarioCatalog.fleeRoute(6, 3)).containsExactly(10, 9, 5);
    }
}
