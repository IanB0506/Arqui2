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

    /** ShedLock con la hora de la base de datos: evita problemas si los relojes de los nodos difieren. */
    @Bean
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

    /**
     * Manejo de errores de los listeners por registro (alertas), comun a todos los servicios (ADR-006): si la
     * base o Redis caen, se reintenta SIN LIMITE con espera acotada a 30 s; una alerta nunca termina en la
     * DLQ por una caida, por larga que sea. Un mensaje mal formado va directo a la DLQ.
     */
    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, String> template, MeterRegistry metrics,
                                            @Value("${spring.application.name}") String service) {
        return ResilientErrorHandlers.create(template, ResilientErrorHandlers.Settings.defaults(service),
                (topic, kind) -> metrics.counter("alerts.dlq", "topic", topic, "error_class", kind.name()).increment(),
                (topic, kind) -> metrics.counter("alerts.retries", "error_class", kind.name()).increment());
    }

    /**
     * Fabrica de listeners por lotes para guardar eventos: inserta cientos de filas por transaccion.
     * Si la base de datos cae, reintenta indefinidamente cada 5 s: los eventos esperan en Kafka
     * (retencion 72 h) y se escriben al volver la base, sin perder ninguno.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<Object, Object> batchFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer, ConsumerFactory<Object, Object> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, consumerFactory);
        factory.setBatchListener(true);
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(5000, FixedBackOff.UNLIMITED_ATTEMPTS)));
        return factory;
    }

    /** Tareas periodicas: una falla por dependencia caida se resume en una linea, sin traza (ADR-006). */
    @Bean
    ThreadPoolTaskSchedulerCustomizer scheduledTaskErrors() {
        return scheduler -> scheduler.setErrorHandler(new ScheduledTaskErrorHandler());
    }
}
