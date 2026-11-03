package gt.umg.centinela.common.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class ZonePartitioner implements Partitioner {
    private static final int MAX_ZONE = 9_999;

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        return partitionFor(key, keyBytes, cluster.partitionsForTopic(topic).size());
    }

    static int partitionFor(Object key, byte[] keyBytes, int partitions) {
        int zone = zoneOf(key);
        if (zone > 0) {
            // La zona N va a la partición N − 1: con el hash de Kafka, las 12 zonas caían en solo 7 particiones.
            return Math.floorMod(zone - 1, partitions);
        }
        if (keyBytes != null) {
            return Utils.toPositive(Utils.murmur2(keyBytes)) % partitions;
        }
        return ThreadLocalRandom.current().nextInt(partitions);
    }

    static int zoneOf(Object key) {
        if (!(key instanceof String text) || text.isEmpty() || text.length() > 4) {
            return 0;
        }
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c < '0' || c > '9') {
                return 0;
            }
        }
        int zone = Integer.parseInt(text);
        return zone <= MAX_ZONE ? zone : 0;
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public void close() {
    }
}
