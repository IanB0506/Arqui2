package gt.umg.centinela.common.kafka;

import gt.umg.centinela.common.json.InvalidEventException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.support.serializer.DeserializationException;

import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

class FailureClassifierTest {

    @Test
    void eventoInvalidoEnvueltoPorElListenerEsPermanente() {
        Throwable e = new ListenerExecutionFailedException("fallo", new InvalidEventException("zona desconocida"));
        assertThat(FailureClassifier.classify(e)).isEqualTo(FailureKind.PERMANENTE);
    }

    @Test
    void timeoutDeKafkaEnvueltoEsTransitorio() {
        Throwable e = new IllegalStateException("no se pudo publicar",
                new ExecutionException(new KafkaException("envio fallido",
                        new org.apache.kafka.common.errors.TimeoutException("sin metadatos"))));
        assertThat(FailureClassifier.classify(e)).isEqualTo(FailureKind.TRANSITORIO);
        assertThat(FailureClassifier.classify(new KafkaException("x", new NetworkException("red")))).isEqualTo(FailureKind.TRANSITORIO);
        assertThat(FailureClassifier.classify(new java.util.concurrent.TimeoutException())).isEqualTo(FailureKind.TRANSITORIO);
    }

    @Test
    void redisYBaseDeDatosCaidosSonTransitorios() {
        assertThat(FailureClassifier.isTransient(new DataAccessResourceFailureException("redis caido"))).isTrue();
        assertThat(FailureClassifier.isTransient(new QueryTimeoutException("timeout de redis"))).isTrue();
        assertThat(FailureClassifier.isTransient(new CannotGetJdbcConnectionException("sin conexion"))).isTrue();
        assertThat(FailureClassifier.isTransient(new SQLException("conexion rechazada", "08001"))).isTrue();
        assertThat(FailureClassifier.isTransient(new SQLException("apagando", "57P01"))).isTrue();
        assertThat(FailureClassifier.isTransient(new SQLException("sintaxis", "42601"))).isFalse();
    }

    @Test
    void erroresDeFormatoSonPermanentes() {
        assertThat(FailureClassifier.classify(new DeserializationException("json roto", new byte[0], false, null)))
                .isEqualTo(FailureKind.PERMANENTE);
        assertThat(FailureClassifier.classify(new RecordTooLargeException("grande"))).isEqualTo(FailureKind.PERMANENTE);
    }

    @Test
    void unErrorDeProgramacionEsDesconocido() {
        assertThat(FailureClassifier.classify(new NullPointerException())).isEqualTo(FailureKind.DESCONOCIDO);
    }

    @Test
    void siHayPermanenteYTransitorioEnLaCadenaGanaPermanente() {
        Throwable e = new DataAccessResourceFailureException("x", new InvalidEventException("malo"));
        assertThat(FailureClassifier.classify(e)).isEqualTo(FailureKind.PERMANENTE);
    }

    @Test
    void unaCausaCiclicaNoProduceUnBucle() {
        RuntimeException a = new RuntimeException("a");
        RuntimeException b = new RuntimeException("b", a);
        a.initCause(b);
        assertThat(FailureClassifier.classify(a)).isEqualTo(FailureKind.DESCONOCIDO);
        assertThat(FailureClassifier.rootCause(a)).isNotNull();
    }
}
