package gt.umg.centinela.alerts.dlq;

import gt.umg.centinela.common.EventHeaders;
import gt.umg.centinela.common.kafka.DlqHeaders;
import gt.umg.centinela.common.kafka.FailureKind;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.KafkaHeaders;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class DlqReprocessorTest {

    private static ConsumerRecord<String, String> dlqRecord(String errorClass, String attempts) {
        ConsumerRecord<String, String> r = new ConsumerRecord<>("events.dlq", 0, 5, "dev-1", "{\"event_id\":\"x\"}");
        r.headers().add(EventHeaders.FIRST_ATTEMPT, "1700000000000".getBytes(StandardCharsets.UTF_8));
        r.headers().add(KafkaHeaders.DLT_ORIGINAL_TOPIC, "sensor.panic.raw".getBytes(StandardCharsets.UTF_8));
        r.headers().add(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP, ByteBuffer.allocate(8).putLong(1700000000123L).array());
        r.headers().add(KafkaHeaders.DLT_EXCEPTION_MESSAGE, "Redis caido".getBytes(StandardCharsets.UTF_8));
        r.headers().add(DlqHeaders.ERROR_CLASS, errorClass.getBytes(StandardCharsets.UTF_8));
        r.headers().add(DlqHeaders.SERVICE, "ingestion-service".getBytes(StandardCharsets.UTF_8));
        if (attempts != null) {
            r.headers().add(DlqHeaders.ATTEMPTS, attempts.getBytes(StandardCharsets.UTF_8));
        }
        return r;
    }

    @Test
    void alReinyectarSeConservaLaHoraOriginalYSeCuentaElIntento() {
        ProducerRecord<String, String> out = DlqReprocessor.republished(dlqRecord("TRANSITORIO", "1"), "sensor.panic.raw", 2);
        assertThat(out.topic()).isEqualTo("sensor.panic.raw");
        assertThat(out.timestamp()).isEqualTo(1700000000123L);
        assertThat(out.key()).isEqualTo("dev-1");
        assertThat(new String(out.headers().lastHeader(DlqHeaders.ATTEMPTS).value(), StandardCharsets.UTF_8)).isEqualTo("2");
        assertThat(out.headers().lastHeader(EventHeaders.FIRST_ATTEMPT)).isNotNull();
        assertThat(out.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC)).isNull();
        assertThat(out.headers().lastHeader(DlqHeaders.ERROR_CLASS)).isNull();
        assertThat(out.headers().headers(DlqHeaders.ATTEMPTS)).hasSize(1);
    }

    @Test
    void laClaseDeFallaSeLeeDelHeaderYSiFaltaEsDesconocida() {
        assertThat(DlqReprocessor.kind(dlqRecord("PERMANENTE", null))).isEqualTo(FailureKind.PERMANENTE);
        assertThat(DlqReprocessor.kind(new ConsumerRecord<>("events.dlq", 0, 1, "k", "v"))).isEqualTo(FailureKind.DESCONOCIDO);
    }

    @Test
    void laCausaSeMuestraSinElEnvoltorioDeSpring() {
        String raw = "Listener method 'public void gt.umg.centinela.ingestion.pipeline.RawEventListener.panic("
                + "org.apache.kafka.clients.consumer.ConsumerRecord<java.lang.String, java.lang.String>)' threw exception; "
                + "Timestamp en el futuro: 2026-10-15T21:31:25Z";
        assertThat(DlqReprocessor.cause(raw)).isEqualTo("Timestamp en el futuro: 2026-10-15T21:31:25Z");
        assertThat(DlqReprocessor.cause(null)).isNull();
    }
}
