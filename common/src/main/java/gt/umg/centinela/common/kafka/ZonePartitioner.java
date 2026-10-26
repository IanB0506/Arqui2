package gt.umg.centinela.common.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Reparte los mensajes cuya llave es una zona ("1".."12") de forma pareja: la zona N va siempre a la
 * particion (N - 1) mod P. Con 12 zonas y 12 particiones cada zona tiene su particion y los 12 hilos de
 * consumo trabajan. El hash por defecto de Kafka (murmur2) dejaba las 12 llaves en solo 7 particiones:
 * 5 hilos sin trabajo y otros con tres zonas.
 *
 * <p>Cualquier otra llave (id de sensor o dispositivo) se reparte igual que el particionador de Kafka, asi
 * que el orden por llave se mantiene. Se activa con {@code partitioner.class} en cada productor.
 */
public class ZonePartitioner implements Partitioner {

    private static final int MAX_ZONE = 9_999;

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        return partitionFor(key, keyBytes, cluster.partitionsForTopic(topic).size());
    }

    static int partitionFor(Object key, byte[] keyBytes, int partitions) {
        int zone = zoneOf(key);
        if (zone > 0) {
            return Math.floorMod(zone - 1, partitions);
        }
        if (keyBytes != null) {
            return Utils.toPositive(Utils.murmur2(keyBytes)) % partitions; // igual que Kafka por defecto
        }
        return ThreadLocalRandom.current().nextInt(partitions);
    }

    /** Numero de zona si la llave es un entero positivo pequeno; si no, 0. */
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
        // sin configuracion
    }

    @Override
    public void close() {
        // sin recursos
    }
}
