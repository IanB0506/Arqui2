package gt.umg.centinela.correlation.config;

import gt.umg.centinela.common.Topics;
import gt.umg.centinela.common.json.InvalidEventException;
import gt.umg.centinela.correlation.engine.CorrelationEngine;
import gt.umg.centinela.correlation.engine.CorrelationState;
import gt.umg.centinela.correlation.engine.ThresholdAdjustments;
import gt.umg.centinela.correlation.engine.WindowStore;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

import java.time.Clock;
import java.time.Duration;

@Configuration
public class EngineConfig {

    @Bean
    public CorrelationEngine correlationEngine(CorrelationProperties properties, WindowStore windows,
                                               CorrelationState state, ThresholdAdjustments adjustments) {
        return new CorrelationEngine(properties, windows, state, adjustments, Clock.systemUTC());
    }

    @Bean
    public KafkaAdmin.NewTopics correlationTopics() {
        return new KafkaAdmin.NewTopics(
                TopicBuilder.name(Topics.NORMALIZED).partitions(Topics.PARTITIONS).replicas(1)
                        .config("retention.ms", String.valueOf(Duration.ofHours(72).toMillis())).build(),
                TopicBuilder.name(Topics.ALERTS).partitions(Topics.ALERT_PARTITIONS).replicas(1)
                        .config("retention.ms", String.valueOf(Duration.ofDays(7).toMillis())).build(),
                TopicBuilder.name(Topics.DLQ).partitions(1).replicas(1)
                        .config("retention.ms", String.valueOf(Duration.ofDays(7).toMillis())).build());
    }

    /**
     * Si Redis no responde, el motor reintenta con espera creciente hasta 2 minutos antes de mandar el
     * evento a la DLQ: para correlacionar es preferible esperar a que Redis vuelva que perder eventos.
     */
    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, String> template) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template,
                (record, exception) -> new TopicPartition(Topics.DLQ, -1));
        ExponentialBackOff backOff = new ExponentialBackOff(500, 2.0);
        backOff.setMaxInterval(10_000);
        backOff.setMaxElapsedTime(120_000);
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        handler.addNotRetryableExceptions(InvalidEventException.class);
        return handler;
    }
}
