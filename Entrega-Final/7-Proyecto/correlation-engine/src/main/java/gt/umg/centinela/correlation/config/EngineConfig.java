package gt.umg.centinela.correlation.config;

import gt.umg.centinela.common.Topics;
import gt.umg.centinela.common.kafka.ResilientErrorHandlers;
import gt.umg.centinela.common.scheduling.ScheduledTaskErrorHandler;
import gt.umg.centinela.correlation.engine.CorrelationEngine;
import gt.umg.centinela.correlation.engine.CorrelationState;
import gt.umg.centinela.correlation.engine.ThresholdAdjustments;
import gt.umg.centinela.correlation.engine.WindowStore;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.task.ThreadPoolTaskSchedulerCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;

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

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, String> template, MeterRegistry metrics,
                                            @Value("${spring.application.name}") String service) {
        return ResilientErrorHandlers.create(template, ResilientErrorHandlers.Settings.defaults(service),
                (topic, kind) -> metrics.counter("correlation.dlq", "error_class", kind.name()).increment(),
                (topic, kind) -> metrics.counter("correlation.retries", "error_class", kind.name()).increment());
    }

    @Bean
    ThreadPoolTaskSchedulerCustomizer scheduledTaskErrors() {
        return scheduler -> scheduler.setErrorHandler(new ScheduledTaskErrorHandler());
    }
}
