package gt.umg.centinela.alerts.config;

import gt.umg.centinela.common.Topics;
import gt.umg.centinela.common.kafka.ResilientErrorHandlers;
import gt.umg.centinela.common.privacy.PlatePseudonymizer;
import gt.umg.centinela.common.scheduling.ScheduledTaskErrorHandler;
import io.micrometer.core.instrument.MeterRegistry;
import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.provider.jdbctemplate.JdbcTemplateLockProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.task.ThreadPoolTaskSchedulerCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import javax.sql.DataSource;
import java.time.Duration;

@Configuration
public class InfrastructureConfig {
    @Bean
    public PlatePseudonymizer platePseudonymizer(AlertServiceProperties properties) {
        return new PlatePseudonymizer(properties.plateHmacSecret());
    }

    @Bean
    // ShedLock con la hora de la base: cada tarea periódica corre en una sola instancia (ADR-004).
    public LockProvider lockProvider(DataSource dataSource) {
        return new JdbcTemplateLockProvider(JdbcTemplateLockProvider.Configuration.builder()
                .withJdbcTemplate(new JdbcTemplate(dataSource))
                .usingDbTime()
                .build());
    }

    @Bean
    public KafkaAdmin.NewTopics alertServiceTopics() {
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
                (topic, kind) -> metrics.counter("alerts.dlq", "topic", topic, "error_class", kind.name()).increment(),
                (topic, kind) -> metrics.counter("alerts.retries", "error_class", kind.name()).increment());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Object, Object> batchFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer, ConsumerFactory<Object, Object> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, consumerFactory);
        factory.setBatchListener(true);
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(5000, FixedBackOff.UNLIMITED_ATTEMPTS)));
        return factory;
    }

    @Bean
    ThreadPoolTaskSchedulerCustomizer scheduledTaskErrors() {
        return scheduler -> scheduler.setErrorHandler(new ScheduledTaskErrorHandler());
    }
}
