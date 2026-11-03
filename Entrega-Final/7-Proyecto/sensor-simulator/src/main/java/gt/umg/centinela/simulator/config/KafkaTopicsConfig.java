package gt.umg.centinela.simulator.config;

import gt.umg.centinela.common.Topics;
import gt.umg.centinela.common.model.SensorType;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.time.Duration;

@Configuration
public class KafkaTopicsConfig {
    @Bean
    public KafkaAdmin.NewTopics rawTopics() {
        NewTopic[] topics = new NewTopic[SensorType.values().length];
        int i = 0;
        for (SensorType type : SensorType.values()) {
            topics[i++] = TopicBuilder.name(type.rawTopic())
                    .partitions(Topics.PARTITIONS)
                    .replicas(1)
                    .config("retention.ms", String.valueOf(Duration.ofHours(24).toMillis()))
                    .build();
        }
        return new KafkaAdmin.NewTopics(topics);
    }
}
