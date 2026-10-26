package gt.umg.centinela.simulator.watchdog;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

/** Consulta a Kafka: brokers vivos y, por grupo de consumidores, miembros (con su nodo) y atraso. */
final class KafkaProbe implements AutoCloseable {

    record GroupInfo(String state, List<String> consumers, Long lag) {
    }

    record KafkaObservation(HealthState state, Integer brokers, String detail, Map<String, GroupInfo> groups) {
    }

    private final AdminClient admin;
    private final long timeoutMs;

    KafkaProbe(Map<String, Object> baseConfig, String clientId, Duration timeout) {
        Map<String, Object> config = new HashMap<>(baseConfig);
        config.put(AdminClientConfig.CLIENT_ID_CONFIG, clientId);
        config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) timeout.toMillis());
        config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) timeout.toMillis());
        config.put(AdminClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 5000);
        this.admin = AdminClient.create(config);
        this.timeoutMs = timeout.toMillis();
    }

    KafkaObservation probe(List<String> groups) {
        int brokers;
        try {
            brokers = admin.describeCluster(new DescribeClusterOptions().timeoutMs((int) timeoutMs)).nodes()
                    .get(timeoutMs, TimeUnit.MILLISECONDS).size();
        } catch (Exception e) {
            return new KafkaObservation(HealthState.DOWN, null, "Kafka no responde: " + rootMessage(e), Map.of());
        }
        Map<String, GroupInfo> result = new LinkedHashMap<>();
        try {
            Map<String, ConsumerGroupDescription> described = admin.describeConsumerGroups(groups).all()
                    .get(timeoutMs, TimeUnit.MILLISECONDS);
            Map<String, Map<TopicPartition, OffsetAndMetadata>> committed = new HashMap<>();
            Map<TopicPartition, OffsetSpec> latest = new HashMap<>();
            for (String group : groups) {
                Map<TopicPartition, OffsetAndMetadata> offsets = admin.listConsumerGroupOffsets(group)
                        .partitionsToOffsetAndMetadata().get(timeoutMs, TimeUnit.MILLISECONDS);
                committed.put(group, offsets);
                offsets.keySet().forEach(tp -> latest.put(tp, OffsetSpec.latest()));
            }
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> ends = latest.isEmpty() ? Map.of()
                    : admin.listOffsets(latest).all().get(timeoutMs, TimeUnit.MILLISECONDS);
            for (String group : groups) {
                ConsumerGroupDescription d = described.get(group);
                TreeSet<String> consumers = new TreeSet<>();
                if (d != null) {
                    for (MemberDescription m : d.members()) {
                        consumers.add(m.clientId().replaceAll("-\\d+$", "")); // correlation-engine-nodo-c-2 -> ...-nodo-c
                    }
                }
                long lag = 0;
                for (Map.Entry<TopicPartition, OffsetAndMetadata> e : committed.get(group).entrySet()) {
                    ListOffsetsResult.ListOffsetsResultInfo end = ends.get(e.getKey());
                    if (end != null && e.getValue() != null) {
                        lag += Math.max(0, end.offset() - e.getValue().offset());
                    }
                }
                result.put(group, new GroupInfo(d == null ? "DESCONOCIDO" : String.valueOf(d.state()),
                        new ArrayList<>(consumers), lag));
            }
        } catch (Exception e) {
            for (String group : groups) {
                result.putIfAbsent(group, new GroupInfo("DESCONOCIDO", List.of(), null));
            }
            return new KafkaObservation(HealthState.UP, brokers, "brokers OK; grupos sin datos: " + rootMessage(e), result);
        }
        return new KafkaObservation(HealthState.UP, brokers, brokers + " broker(s)", result);
    }

    private static String rootMessage(Throwable e) {
        Throwable t = e;
        while (t.getCause() != null && t.getCause() != t) {
            t = t.getCause();
        }
        return t.getClass().getSimpleName() + (t.getMessage() == null ? "" : ": " + t.getMessage());
    }

    @Override
    public void close() {
        admin.close(Duration.ofSeconds(2));
    }
}
