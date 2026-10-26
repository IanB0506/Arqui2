package gt.umg.centinela.simulator.publish;

import gt.umg.centinela.common.EventHeaders;
import gt.umg.centinela.common.model.raw.PanicAlertType;
import gt.umg.centinela.simulator.config.SimulatorProperties;
import gt.umg.centinela.simulator.generator.EventFactory;
import gt.umg.centinela.simulator.generator.SimulatedEvent;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class EventPublisherTest {

    @TempDir
    Path dir;

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
    private final SimpleMeterRegistry metrics = new SimpleMeterRegistry();
    private SpoolService spool;
    private EventPublisher publisher;
    private EventFactory factory;

    @BeforeEach
    void setUp() {
        SimulatorProperties properties = new SimulatorProperties();
        properties.setSpoolDir(dir.toString());
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.initialize();
        spool = new SpoolService(properties, kafka, metrics, scheduler);
        publisher = new EventPublisher(kafka, spool, new SpoolPolicy(properties), metrics);
        factory = new EventFactory(properties);
    }

    @Test
    void elCriticoQuedaEnDiscoAntesDeEnviarseYSeBorraAlConfirmar() {
        AtomicLong pendingDuringSend = new AtomicLong(-1);
        CompletableFuture<SendResult<String, String>> future = new CompletableFuture<>();
        when(kafka.send(any(ProducerRecord.class))).thenAnswer(inv -> {
            pendingDuringSend.set(spool.pendingCount());
            return future;
        });
        publisher.publish(factory.panic(4, PanicAlertType.ROBO, "dev-1"));
        assertThat(pendingDuringSend.get()).isEqualTo(1); // write-ahead
        future.complete(null);
        assertThat(spool.pendingCount()).isZero();
    }

    @Test
    void siKafkaFallaElCriticoSigueEnElSpoolYLaTelemetriaUtilTambien() {
        when(kafka.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.failedFuture(new TimeoutException("caido")));
        publisher.publish(factory.panic(4, PanicAlertType.ROBO, "dev-1"));
        publisher.publish(factory.speed(4, 1, 85, false)); // exceso: util
        publisher.publish(factory.speed(4, 1, 45, false)); // normal: se descarta
        assertThat(spool.pendingCount()).isEqualTo(1);
        assertThat(spool.telemetryPending()).isEqualTo(1);
        assertThat(metrics.counter("simulator.events.sent", "sensor_type", "SPEED", "result", "dropped").count()).isEqualTo(1);
    }

    @Test
    void elMensajeLlevaLaHoraDelPrimerIntentoYElReenvioLaConserva() {
        when(kafka.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.failedFuture(new TimeoutException("caido")));
        SimulatedEvent panic = factory.panic(4, PanicAlertType.ROBO, "dev-1");
        publisher.publish(panic);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<ProducerRecord<String, String>> sent = ArgumentCaptor.forClass(ProducerRecord.class);
        when(kafka.send(sent.capture())).thenReturn(CompletableFuture.completedFuture(null));
        spool.resend();
        ProducerRecord<String, String> resent = sent.getValue();
        String firstAttempt = new String(resent.headers().lastHeader(EventHeaders.FIRST_ATTEMPT).value(), StandardCharsets.UTF_8);
        assertThat(resent.timestamp()).isEqualTo(Long.parseLong(firstAttempt));
        assertThat(new String(resent.headers().lastHeader(EventHeaders.SPOOLED).value(), StandardCharsets.UTF_8)).isEqualTo("true");
        assertThat(spool.pendingCount()).isZero();
    }
}
