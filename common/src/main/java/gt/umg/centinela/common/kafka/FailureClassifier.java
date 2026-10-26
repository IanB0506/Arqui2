package gt.umg.centinela.common.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import gt.umg.centinela.common.json.InvalidEventException;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.SerializationException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.kafka.listener.ExceptionClassifier;
import org.springframework.transaction.CannotCreateTransactionException;

import java.io.UncheckedIOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

/**
 * Clasifica una falla recorriendo toda su cadena de causas (ADR-006).
 *
 * <ul>
 *   <li>PERMANENTE: el mensaje no cumple el contrato o no se puede serializar. Reintentar es inutil:
 *       va directo a la DLQ.</li>
 *   <li>TRANSITORIO: Kafka, Redis o la base de datos no responden. Se reintenta sin limite, con
 *       espera acotada, hasta que la dependencia vuelva: asi una caida no termina en la DLQ.</li>
 *   <li>DESCONOCIDO: cualquier otra cosa (un error de programacion). Pocos reintentos y luego DLQ,
 *       para que un bug no bloquee una particion para siempre.</li>
 * </ul>
 * Si alguna causa de la cadena es permanente, gana sobre las transitorias.
 */
public final class FailureClassifier {

    private static final int MAX_DEPTH = 20;

    private static final List<Class<? extends Throwable>> PERMANENT = buildPermanent();

    private static final List<Class<? extends Throwable>> TRANSIENT = List.of(
            RetriableException.class,                  // TimeoutException, NetworkException, NotLeaderOrFollower...
            java.util.concurrent.TimeoutException.class,
            InterruptedException.class,
            TransientDataAccessException.class,        // incluye QueryTimeoutException (timeout de Redis)
            RecoverableDataAccessException.class,
            DataAccessResourceFailureException.class,  // RedisConnectionFailureException, CannotGetJdbcConnectionException
            CannotCreateTransactionException.class,
            SQLTransientException.class,               // incluye SQLTransientConnectionException de Hikari
            SQLRecoverableException.class,
            ConnectException.class,
            NoRouteToHostException.class,
            UnknownHostException.class,
            SocketTimeoutException.class);

    /** Clases de bibliotecas opcionales que se reconocen por nombre (Lettuce no esta en common). */
    private static final Set<String> TRANSIENT_BY_NAME = Set.of(
            "io.lettuce.core.RedisConnectionException",
            "io.lettuce.core.RedisCommandTimeoutException");

    private FailureClassifier() {
    }

    private static List<Class<? extends Throwable>> buildPermanent() {
        List<Class<? extends Throwable>> list = new ArrayList<>(List.of(
                InvalidEventException.class,
                JsonProcessingException.class,
                RecordTooLargeException.class,
                SerializationException.class,
                InvalidTopicException.class,
                AuthorizationException.class,
                InvalidRecordException.class));
        // DeserializationException, MessageConversionException, ConversionException,
        // MethodArgumentResolutionException, NoSuchMethodException, ClassCastException
        list.addAll(ExceptionClassifier.defaultFatalExceptionsList());
        return Collections.unmodifiableList(list);
    }

    public static FailureKind classify(Throwable error) {
        boolean transientFound = false;
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        Throwable current = error;
        for (int depth = 0; current != null && depth < MAX_DEPTH && seen.add(current); depth++) {
            if (isInstance(PERMANENT, current)) {
                return FailureKind.PERMANENTE;
            }
            if (isTransientLevel(current)) {
                transientFound = true;
            }
            current = current.getCause();
        }
        return transientFound ? FailureKind.TRANSITORIO : FailureKind.DESCONOCIDO;
    }

    public static boolean isTransient(Throwable error) {
        return classify(error) == FailureKind.TRANSITORIO;
    }

    public static boolean isPermanent(Throwable error) {
        return classify(error) == FailureKind.PERMANENTE;
    }

    /** Excepciones permanentes propias, para {@code DefaultErrorHandler.addNotRetryableExceptions}. */
    @SuppressWarnings("unchecked")
    public static Class<? extends Exception>[] permanentExceptionTypes() {
        return new Class[]{InvalidEventException.class, JsonProcessingException.class, RecordTooLargeException.class,
                SerializationException.class, InvalidTopicException.class, AuthorizationException.class,
                InvalidRecordException.class};
    }

    /** Causa mas profunda de la cadena (sin entrar en ciclos). */
    public static Throwable rootCause(Throwable error) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        Throwable current = error;
        while (current.getCause() != null && seen.add(current) && seen.size() < MAX_DEPTH) {
            current = current.getCause();
        }
        return current;
    }

    private static boolean isTransientLevel(Throwable t) {
        if (isInstance(TRANSIENT, t) || TRANSIENT_BY_NAME.contains(t.getClass().getName())) {
            return true;
        }
        if (t instanceof UncheckedIOException) {
            return true;
        }
        // PostgreSQL: SQLState 08xxx (conexion) y 57P0x (servidor apagandose o arrancando)
        if (t instanceof SQLException sql && sql.getSQLState() != null) {
            String state = sql.getSQLState();
            return state.startsWith("08") || state.startsWith("57P");
        }
        return false;
    }

    private static boolean isInstance(List<Class<? extends Throwable>> types, Throwable t) {
        for (Class<? extends Throwable> type : types) {
            if (type.isInstance(t)) {
                return true;
            }
        }
        return false;
    }
}
