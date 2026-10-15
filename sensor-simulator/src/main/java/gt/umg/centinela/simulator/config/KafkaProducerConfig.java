package gt.umg.centinela.simulator.config;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.ProducerListener;

@Configuration
public class KafkaProducerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerConfig.class);

    /**
     * Reemplaza al LoggingProducerListener de Spring Boot, que escribe un ERROR con el payload completo
     * (placas e ids de ciudadanos en claro) por cada envio fallido. Con Kafka caido eso es ruido y
     * ademas una fuga de privacidad (ADR-007). EventPublisher ya resume las fallas.
     */
    @Bean
    public ProducerListener<Object, Object> kafkaProducerListener(MeterRegistry metrics) {
        return new ProducerListener<>() {
            @Override
            public void onError(ProducerRecord<Object, Object> record, RecordMetadata metadata, Exception exception) {
                metrics.counter("simulator.kafka.send.errors", "topic", record.topic()).increment();
                log.debug("Envio fallido a {}: {}", record.topic(), exception.getMessage());
            }
        };
    }

    /** Con Kafka caido, cerrar el productor esperaria 30 s; los criticos ya estan en disco. */
    @Bean
    public DefaultKafkaProducerFactoryCustomizer producerCloseTimeout() {
        return factory -> factory.setPhysicalCloseTimeout(5);
    }
}
