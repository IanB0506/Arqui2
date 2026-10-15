package gt.umg.centinela.ingestion.pipeline;

import gt.umg.centinela.common.Topics;
import gt.umg.centinela.common.model.SensorType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consume los cinco topics crudos. Un listener por topic: asi una avalancha de LPR no bloquea el
 * procesamiento de botones de panico (cada topic tiene sus propios hilos consumidores).
 */
@Component
public class RawEventListener {

    private final IngestionPipeline pipeline;

    public RawEventListener(IngestionPipeline pipeline) {
        this.pipeline = pipeline;
    }

    @KafkaListener(topics = Topics.RAW_PANIC, concurrency = "${ingestion.concurrency.panic:2}")
    public void panic(ConsumerRecord<String, String> record) {
        pipeline.process(SensorType.PANIC, record);
    }

    @KafkaListener(topics = Topics.RAW_LPR, concurrency = "${ingestion.concurrency.lpr:6}")
    public void lpr(ConsumerRecord<String, String> record) {
        pipeline.process(SensorType.LPR, record);
    }

    @KafkaListener(topics = Topics.RAW_SPEED, concurrency = "${ingestion.concurrency.speed:6}")
    public void speed(ConsumerRecord<String, String> record) {
        pipeline.process(SensorType.SPEED, record);
    }

    @KafkaListener(topics = Topics.RAW_ACOUSTIC, concurrency = "${ingestion.concurrency.acoustic:2}")
    public void acoustic(ConsumerRecord<String, String> record) {
        pipeline.process(SensorType.ACOUSTIC, record);
    }

    @KafkaListener(topics = Topics.RAW_CITIZEN, concurrency = "${ingestion.concurrency.citizen:2}")
    public void citizen(ConsumerRecord<String, String> record) {
        pipeline.process(SensorType.CITIZEN_REPORT, record);
    }
}
