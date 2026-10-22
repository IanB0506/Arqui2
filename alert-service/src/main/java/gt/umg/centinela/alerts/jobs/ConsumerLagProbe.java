package gt.umg.centinela.alerts.jobs;

import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Atraso (lag) de los grupos de consumidores del camino de deteccion, para el monitor de SLO. */
@Component
public class ConsumerLagProbe {

    private static final Logger log = LoggerFactory.getLogger(ConsumerLagProbe.class);
    /** Camino completo: ingesta, correlacion y las dos persistencias (eventos y alertas). */
    static final List<String> GROUPS = List.of("ingestion", "correlation", "event-store", "alert-store");

    private final KafkaAdmin kafkaAdmin;
    private AdminClient admin;

    public ConsumerLagProbe(KafkaAdmin kafkaAdmin) {
        this.kafkaAdmin = kafkaAdmin;
    }

    /** Suma del atraso de los grupos; null si Kafka no responde. */
    public synchronized Long totalLag() {
        try {
            AdminClient client = client();
            Map<TopicPartition, Long> committed = new HashMap<>();
            for (String group : GROUPS) {
                Map<TopicPartition, OffsetAndMetadata> offsets = client.listConsumerGroupOffsets(group)
                        .partitionsToOffsetAndMetadata().get(3, TimeUnit.SECONDS);
                offsets.forEach((tp, om) -> {
                    if (om != null) {
                        committed.put(new TopicPartition(group + "|" + tp.topic(), tp.partition()), om.offset());
                    }
                });
            }
            if (committed.isEmpty()) {
                return 0L;
            }
            // varios grupos leen el mismo topic (events.normalized): la llave interna lleva el grupo
            Map<TopicPartition, OffsetSpec> latest = new HashMap<>();
            committed.keySet().forEach(tp -> latest.put(real(tp), OffsetSpec.latest()));
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> ends = client.listOffsets(latest).all()
                    .get(3, TimeUnit.SECONDS);
            long lag = 0;
            for (Map.Entry<TopicPartition, Long> e : committed.entrySet()) {
                ListOffsetsResult.ListOffsetsResultInfo end = ends.get(real(e.getKey()));
                if (end != null) {
                    lag += Math.max(0, end.offset() - e.getValue());
                }
            }
            return lag;
        } catch (Exception e) {
            log.debug("No se pudo medir el atraso de los consumidores: {}", e.toString());
            return null;
        }
    }

    private static TopicPartition real(TopicPartition grouped) {
        return new TopicPartition(grouped.topic().substring(grouped.topic().indexOf('|') + 1), grouped.partition());
    }

    private AdminClient client() {
        if (admin == null) {
            Map<String, Object> config = new HashMap<>(kafkaAdmin.getConfigurationProperties());
            config.put(AdminClientConfig.CLIENT_ID_CONFIG, "alert-service-slo-lag");
            config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);
            config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 3000);
            admin = AdminClient.create(config);
        }
        return admin;
    }

    @PreDestroy
    public synchronized void close() {
        if (admin != null) {
            admin.close();
        }
    }
}
