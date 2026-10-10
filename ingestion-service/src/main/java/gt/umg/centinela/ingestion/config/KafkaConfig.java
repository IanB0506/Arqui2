package gt.umg.centinela.ingestion.config;

import gt.umg.centinela.common.Topics;
import gt.umg.centinela.common.json.InvalidEventException;
import gt.umg.centinela.common.model.SensorType;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Topics de salida y manejo de errores de la ingesta (ADR-006 Resiliencia).
 *
 * <ul>
 *   <li>Errores transitorios (Kafka o Redis momentaneamente caidos): 4 reintentos con espera
 *       exponencial 1 s, 2 s, 4 s, 8 s.</li>
 *   <li>Errores permanentes (JSON mal formado, campos faltantes, zona inexistente): sin reintentos,
 *       directo a la DLQ {@code events.dlq} con cabeceras que explican la causa.</li>
 * </ul>
 */
@Configuration
public class KafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    @Bean
    public KafkaAdmin.NewTopics ingestionTopics() {
        // Declara tambien los topics crudos que consume, por si arranca antes que el simulador.
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
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, String> template) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template,
                (record, exception) -> {
                    log.warn("Evento enviado a DLQ desde {}: {}", record.topic(), rootMessage(exception));
                    // particion negativa = Kafka elige la particion de la DLQ
                    return new TopicPartition(Topics.DLQ, -1);
                });
        ExponentialBackOff backOff = new ExponentialBackOff(1000, 2.0);
        backOff.setMaxAttempts(4);
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        handler.addNotRetryableExceptions(InvalidEventException.class);
        return handler;
    }

    private static String rootMessage(Throwable e) {
        Throwable t = e;
        while (t.getCause() != null) {
            t = t.getCause();
        }
        return t.getMessage();
    }
}
