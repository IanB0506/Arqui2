package gt.umg.centinela.alerts.dlq;

import gt.umg.centinela.common.Topics;
import gt.umg.centinela.common.kafka.DlqHeaders;
import gt.umg.centinela.common.kafka.FailureKind;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * Reproceso de la cola de mensajes muertos {@code events.dlq} (ADR-006).
 *
 * <p>Con los reintentos sin limite para errores transitorios, a la DLQ solo deberian llegar mensajes
 * mal formados (PERMANENTE) y errores no clasificados (DESCONOCIDO). Aun asi:
 * <ul>
 *   <li>cada 5 min se reinyectan en su topic original los TRANSITORIO y DESCONOCIDO, como mucho 3
 *       veces por mensaje (header {@code centinela-dlq-attempts});</li>
 *   <li>un administrador puede reprocesar a mano, incluso los PERMANENTE (por ejemplo tras corregir un
 *       error), con {@code POST /api/admin/dlq/reprocess?incluirPermanentes=true}.</li>
 * </ul>
 * Al reinyectar se conserva el timestamp y los headers originales: la ingesta mide la edad contra la
 * hora de entrada al sistema y la deduplicacion por event_id descarta lo que ya se habia procesado.
 */
@Component
public class DlqReprocessor {

    private static final Logger log = LoggerFactory.getLogger(DlqReprocessor.class);
    static final int MAX_ATTEMPTS = 3;
    private static final String AUTO_GROUP = "dlq-reprocessor";
    private static final String ORIGINAL_TOPIC = KafkaHeaders.DLT_ORIGINAL_TOPIC;
    private static final String ORIGINAL_TIMESTAMP = KafkaHeaders.DLT_ORIGINAL_TIMESTAMP;
    private static final String EXCEPTION_MESSAGE = KafkaHeaders.DLT_EXCEPTION_MESSAGE;

    public record Result(int reprocessed, int skipped, int read) {
    }

    private final KafkaProperties kafkaProperties;
    private final KafkaTemplate<String, String> kafka;

    public DlqReprocessor(KafkaProperties kafkaProperties, KafkaTemplate<String, String> kafka) {
        this.kafkaProperties = kafkaProperties;
        this.kafka = kafka;
    }

    @Scheduled(fixedDelay = 300_000, initialDelay = 120_000)
    @SchedulerLock(name = "dlq-reprocess", lockAtMostFor = "PT4M")
    public void automatic() {
        Result r = reprocess(false, false);
        if (r.reprocessed() > 0) {
            log.info("DLQ: {} mensajes reinyectados automaticamente ({} omitidos)", r.reprocessed(), r.skipped());
        }
    }

    /**
     * @param includePermanent tambien los PERMANENTE (solo a mano)
     * @param fromBeginning    leer toda la DLQ (a mano) en vez de continuar donde quedo el reproceso automatico
     */
    public synchronized Result reprocess(boolean includePermanent, boolean fromBeginning) {
        int reprocessed = 0;
        int skipped = 0;
        int read = 0;
        try (KafkaConsumer<String, String> consumer = consumer(fromBeginning ? null : AUTO_GROUP)) {
            List<TopicPartition> partitions = partitions(consumer);
            consumer.assign(partitions);
            if (fromBeginning) {
                consumer.seekToBeginning(partitions);
            }
            Map<TopicPartition, Long> end = consumer.endOffsets(partitions);
            while (!reachedEnd(consumer, end)) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofSeconds(2))) {
                    read++;
                    int attempts = intHeader(record, DlqHeaders.ATTEMPTS);
                    FailureKind kind = kind(record);
                    String original = header(record, ORIGINAL_TOPIC);
                    boolean eligible = original != null && attempts < MAX_ATTEMPTS
                            && (kind != FailureKind.PERMANENTE || includePermanent);
                    if (!eligible) {
                        skipped++;
                        continue;
                    }
                    kafka.send(republished(record, original, attempts + 1)).get(10, TimeUnit.SECONDS);
                    reprocessed++;
                }
            }
            if (!fromBeginning) {
                Map<TopicPartition, OffsetAndMetadata> commit = new HashMap<>();
                for (TopicPartition tp : partitions) {
                    commit.put(tp, new OffsetAndMetadata(consumer.position(tp)));
                }
                consumer.commitSync(commit);
            }
        } catch (Exception e) {
            log.warn("DLQ: el reproceso se interrumpio ({}); se continua en la siguiente ronda", e.toString());
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        }
        return new Result(reprocessed, skipped, read);
    }

    /** Resumen de toda la DLQ: cantidades por topic de origen, clase y servicio, y los ultimos mensajes. */
    public synchronized Map<String, Object> summary(int lastN) {
        Map<String, Integer> byOrigin = new TreeMap<>();
        ArrayDeque<Map<String, Object>> last = new ArrayDeque<>();
        int total = 0;
        try (KafkaConsumer<String, String> consumer = consumer(null)) {
            List<TopicPartition> partitions = partitions(consumer);
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, Long> end = consumer.endOffsets(partitions);
            while (!reachedEnd(consumer, end)) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofSeconds(2))) {
                    total++;
                    String key = header(record, ORIGINAL_TOPIC) + " / " + kind(record) + " / " + header(record, DlqHeaders.SERVICE);
                    byOrigin.merge(key, 1, Integer::sum);
                    Map<String, Object> item = new LinkedHashMap<>();
                    item.put("hora", Instant.ofEpochMilli(record.timestamp()).toString());
                    item.put("topicOriginal", header(record, ORIGINAL_TOPIC));
                    item.put("clase", kind(record).name());
                    item.put("servicio", header(record, DlqHeaders.SERVICE));
                    item.put("reprocesos", intHeader(record, DlqHeaders.ATTEMPTS));
                    item.put("causa", cause(header(record, EXCEPTION_MESSAGE)));
                    last.addFirst(item);
                    if (last.size() > lastN) {
                        last.removeLast();
                    }
                }
            }
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("total", total);
        result.put("porOrigenClaseYServicio", byOrigin);
        result.put("ultimos", new ArrayList<>(last));
        return result;
    }

    private KafkaConsumer<String, String> consumer(String group) {
        Map<String, Object> config = new HashMap<>(kafkaProperties.buildConsumerProperties());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.CLIENT_ID_CONFIG, "alert-service-dlq");
        if (group == null) {
            config.remove(ConsumerConfig.GROUP_ID_CONFIG);
        } else {
            config.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        }
        return new KafkaConsumer<>(config);
    }

    private static List<TopicPartition> partitions(KafkaConsumer<String, String> consumer) {
        List<TopicPartition> partitions = new ArrayList<>();
        for (PartitionInfo p : consumer.partitionsFor(Topics.DLQ, Duration.ofSeconds(5))) {
            partitions.add(new TopicPartition(p.topic(), p.partition()));
        }
        return partitions;
    }

    private static boolean reachedEnd(KafkaConsumer<String, String> consumer, Map<TopicPartition, Long> end) {
        for (Map.Entry<TopicPartition, Long> e : end.entrySet()) {
            if (consumer.position(e.getKey()) < e.getValue()) {
                return false;
            }
        }
        return true;
    }

    /** Causa legible: sin el envoltorio "Listener method '...' threw exception; " de Spring Kafka. */
    static String cause(String message) {
        if (message == null) {
            return null;
        }
        String clean = message.replaceFirst("^Listener method '.*?' threw exception; ", "");
        return clean.length() > 200 ? clean.substring(0, 200) : clean;
    }

    static ProducerRecord<String, String> republished(ConsumerRecord<String, String> record, String topic,
                                                              int attempts) {
        Long timestamp = longHeader(record, ORIGINAL_TIMESTAMP);
        ProducerRecord<String, String> out = new ProducerRecord<>(topic, null, timestamp, record.key(), record.value());
        for (Header h : record.headers()) {
            // se conservan los headers originales (p. ej. centinela-first-attempt), no los de la DLQ
            if (!h.key().startsWith("kafka_dlt") && !h.key().startsWith("centinela-error")
                    && !h.key().equals(DlqHeaders.SERVICE) && !h.key().equals(DlqHeaders.ATTEMPTS)) {
                out.headers().add(h);
            }
        }
        out.headers().add(DlqHeaders.ATTEMPTS, Integer.toString(attempts).getBytes(StandardCharsets.UTF_8));
        return out;
    }

    static FailureKind kind(ConsumerRecord<String, String> record) {
        String value = header(record, DlqHeaders.ERROR_CLASS);
        try {
            return value == null ? FailureKind.DESCONOCIDO : FailureKind.valueOf(value);
        } catch (IllegalArgumentException e) {
            return FailureKind.DESCONOCIDO;
        }
    }

    private static String header(ConsumerRecord<String, String> record, String key) {
        Header h = record.headers().lastHeader(key);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    private static int intHeader(ConsumerRecord<String, String> record, String key) {
        try {
            String v = header(record, key);
            return v == null ? 0 : Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private static Long longHeader(ConsumerRecord<String, String> record, String key) {
        Header h = record.headers().lastHeader(key);
        if (h == null || h.value().length != Long.BYTES) {
            return null;
        }
        return ByteBuffer.wrap(h.value()).getLong();
    }
}
