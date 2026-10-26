package gt.umg.centinela.common.kafka;

import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class ZonePartitionerTest {

    private static int partition(String key, int partitions) {
        return ZonePartitioner.partitionFor(key, key == null ? null : key.getBytes(StandardCharsets.UTF_8), partitions);
    }

    @Test
    void cadaZonaTieneSuParticionConDoceParticiones() {
        Set<Integer> used = new HashSet<>();
        for (int zone = 1; zone <= 12; zone++) {
            assertThat(partition(String.valueOf(zone), 12)).isEqualTo(zone - 1);
            used.add(partition(String.valueOf(zone), 12));
        }
        assertThat(used).hasSize(12);
    }

    @Test
    void conTresParticionesCadaUnaRecibeCuatroZonas() {
        Map<Integer, Integer> perPartition = new HashMap<>();
        for (int zone = 1; zone <= 12; zone++) {
            perPartition.merge(partition(String.valueOf(zone), 3), 1, Integer::sum);
        }
        assertThat(perPartition).containsOnly(Map.entry(0, 4), Map.entry(1, 4), Map.entry(2, 4));
    }

    @Test
    void lasDemasLlavesSeRepartenComoElParticionadorDeKafka() {
        for (String key : new String[]{"LPR-Z04-01", "APP-1234", "0", "-3", "12345", "4a"}) {
            byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
            assertThat(partition(key, 12)).as(key).isEqualTo(Utils.toPositive(Utils.murmur2(bytes)) % 12);
        }
    }

    @Test
    void sinLlaveDevuelveUnaParticionValida() {
        for (int i = 0; i < 50; i++) {
            assertThat(partition(null, 12)).isBetween(0, 11);
        }
    }
}
