package gt.umg.centinela.ingestion.api;

import gt.umg.centinela.common.EventHeaders;
import gt.umg.centinela.ingestion.config.GatewayProperties;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class GatewaySpoolTest {

    @TempDir
    Path dir;

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);

    private GatewaySpool gateway() {
        return new GatewaySpool(kafka, new GatewayProperties(dir.toString(), Duration.ofMillis(50), Duration.ofSeconds(10),
                5, 32, true), Clock.systemUTC(), new SimpleMeterRegistry());
    }

    @Test
    void conKafkaArribaSePublicaSinTocarElDisco() {
        when(kafka.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));
        GatewaySpool g = gateway();
        assertThat(g.submit("sensor.panic.raw", "e1", "dev", "{}")).isEqualTo(GatewaySpool.Outcome.PUBLICADO);
        assertThat(g.status()).containsEntry("spoolPendientes", 0);
    }

    @Test
    void siKafkaNoConfirmaQuedaEnColaYUnaConfirmacionTardiaLoBorra() {
        CompletableFuture<SendResult<String, String>> slow = new CompletableFuture<>();
        when(kafka.send(any(ProducerRecord.class))).thenReturn(slow);
        GatewaySpool g = gateway();
        assertThat(g.submit("sensor.panic.raw", "e1", "dev", "{}")).isEqualTo(GatewaySpool.Outcome.EN_COLA);
        assertThat(g.status()).containsEntry("spoolPendientes", 1);
        slow.complete(null);
        assertThat(g.status()).containsEntry("spoolPendientes", 0);
    }

    @Test
    void trasUnaFallaLasPeticionesVanDirectoAlSpoolYSeReenvianConLaHoraOriginal() {
        when(kafka.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.failedFuture(new TimeoutException("caido")));
        GatewaySpool g = gateway();
        assertThat(g.submit("sensor.panic.raw", "e1", "dev", "{}")).isEqualTo(GatewaySpool.Outcome.EN_COLA);
        assertThat(g.submit("sensor.panic.raw", "e2", "dev", "{}")).isEqualTo(GatewaySpool.Outcome.EN_COLA);
        verify(kafka, times(1)).send(any(ProducerRecord.class)); // la segunda no espero a Kafka

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ProducerRecord<String, String>> sent = ArgumentCaptor.forClass(ProducerRecord.class);
        when(kafka.send(sent.capture())).thenReturn(CompletableFuture.completedFuture(null));
        g.resend();
        assertThat(sent.getAllValues()).hasSize(2);
        ProducerRecord<String, String> r = sent.getAllValues().get(0);
        String first = new String(r.headers().lastHeader(EventHeaders.FIRST_ATTEMPT).value(), StandardCharsets.UTF_8);
        assertThat(r.timestamp()).isEqualTo(Long.parseLong(first));
        assertThat(r.headers().lastHeader(EventHeaders.SPOOLED)).isNotNull();
        assertThat(g.status()).containsEntry("spoolPendientes", 0).containsEntry("kafkaDisponible", true);
    }

    @Test
    void loPendienteSobreviveAUnReinicio() {
        when(kafka.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.failedFuture(new TimeoutException("caido")));
        gateway().submit("sensor.panic.raw", "e1", "dev", "{}");
        GatewaySpool after = gateway();
        assertThat(after.status()).containsEntry("spoolPendientes", 1);
        verify(kafka, never()).send(any(String.class), any(String.class), any(String.class));
    }
}
