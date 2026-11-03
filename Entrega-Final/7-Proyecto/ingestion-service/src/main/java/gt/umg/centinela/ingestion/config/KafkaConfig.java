package gt.umg.centinela.ingestion.config;

import gt.umg.centinela.common.Topics;
import gt.umg.centinela.common.kafka.ResilientErrorHandlers;
import gt.umg.centinela.common.model.SensorType;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ProducerListener;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@Configuration
public class KafkaConfig {
    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    @Bean
    public KafkaAdmin.NewTopics ingestionTopics() {
        List<NewTopic> topics = new ArrayList<>();
        for (SensorType type : SensorType.values()) {
            topics.add(TopicBuilder.name(type.rawTopic()).partitions(Topics.PARTITIONS).replicas(1)
                    .config("retention.ms", String.valueOf(Duration.ofHours(24).toMillis())).build());
        }
        topics.add(TopicBuilder.name(Topics.NORMALIZED).partitions(Topics.PARTITIONS).replicas(1)
                .config("retention.ms", String.valueOf(Duration.ofHours(72).toMillis())).build());
        topics.add(TopicBuilder.name(Topics.DLQ).partitions(1).replicas(1)
                .config("retention.ms", String.valueOf(Duration.ofDays(7).toMillis())).build());
        return new KafkaAdmin.NewTopics(topics.toArray(NewTopic[]::new));
    }

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, String> template, ResilienceProperties r,
                                            MeterRegistry metrics, @Value("${spring.application.name}") String service) {
        return ResilientErrorHandlers.create(template,
                new ResilientErrorHandlers.Settings(service, r.transientInitialInterval(), r.transientMaxInterval(),
                        r.unknownMaxAttempts(), r.retryLogEvery()),
                (topic, kind) -> metrics.counter("ingestion.dlq", "topic", topic, "error_class", kind.name()).increment(),
                (topic, kind) -> metrics.counter("ingestion.retries", "error_class", kind.name()).increment());
    }

    @Bean
    public ProducerListener<Object, Object> kafkaProducerListener(MeterRegistry metrics) {
        return new ProducerListener<>() {
            @Override
            public void onError(ProducerRecord<Object, Object> record, RecordMetadata metadata, Exception exception) {
                metrics.counter("ingestion.kafka.send.errors", "topic", record.topic()).increment();
                log.debug("Envio fallido a {}: {}", record.topic(), exception.getMessage());
            }
        };
    }
}
