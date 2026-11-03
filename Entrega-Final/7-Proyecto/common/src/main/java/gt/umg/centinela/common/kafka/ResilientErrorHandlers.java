package gt.umg.centinela.common.kafka;

import gt.umg.centinela.common.Topics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.function.BiConsumer;

public final class ResilientErrorHandlers {
    private static final Logger log = LoggerFactory.getLogger(ResilientErrorHandlers.class);

    public record Settings(String service, Duration initialInterval, Duration maxInterval, int unknownMaxAttempts,
                           int retryLogEvery) {
        public static Settings defaults(String service) {
            return new Settings(service, Duration.ofSeconds(1), Duration.ofSeconds(30), 4, 10);
        }
    }

    private ResilientErrorHandlers() {
    }

    public static DefaultErrorHandler create(KafkaOperations<?, ?> template, Settings settings,
                                             BiConsumer<String, FailureKind> onDlq,
                                             BiConsumer<String, FailureKind> onRetry) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template, (record, exception) -> {
            FailureKind kind = FailureClassifier.classify(exception);
            log.warn("Mensaje de {} enviado a la DLQ ({}): {}", record.topic(), kind, rootMessage(exception));
            onDlq.accept(record.topic(), kind);
            return new TopicPartition(Topics.DLQ, -1);
        });
        recoverer.setHeadersFunction((record, exception) -> {
            RecordHeaders headers = new RecordHeaders();
            headers.add(DlqHeaders.ERROR_CLASS,
                    FailureClassifier.classify(exception).name().getBytes(StandardCharsets.UTF_8));
            headers.add(DlqHeaders.SERVICE, settings.service().getBytes(StandardCharsets.UTF_8));
            return headers;
        });
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, unknownBackOff(settings));
        BackOff transientBackOff = transientBackOff(settings);
        BackOff unknownBackOff = unknownBackOff(settings);
        BackOff noRetries = new FixedBackOff(0, 0);
        handler.setBackOffFunction((record, exception) -> switch (FailureClassifier.classify(exception)) {
            case PERMANENTE -> noRetries;
            case TRANSITORIO -> transientBackOff;
            case DESCONOCIDO -> unknownBackOff;
        });
        handler.setLogLevel(KafkaException.Level.DEBUG);
        handler.setRetryListeners(new RetryListener() {
            @Override
            public void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt) {
                FailureKind kind = FailureClassifier.classify(ex);
                onRetry.accept(record.topic(), kind);
                if (deliveryAttempt == 1 || deliveryAttempt % Math.max(1, settings.retryLogEvery()) == 0) {
                    log.warn("Reintento {} de {}-{}@{} ({}): {}", deliveryAttempt, record.topic(), record.partition(),
                            record.offset(), kind, rootMessage(ex));
                }
            }

            @Override
            public void recoveryFailed(ConsumerRecord<?, ?> record, Exception original, Exception failure) {
                log.warn("No se pudo enviar a la DLQ el mensaje {}-{}@{}: {}", record.topic(), record.partition(),
                        record.offset(), rootMessage(failure));
            }
        });
        return handler;
    }

    static BackOff transientBackOff(Settings settings) {
        ExponentialBackOff backOff = new ExponentialBackOff(settings.initialInterval().toMillis(), 2.0);
        backOff.setMaxInterval(settings.maxInterval().toMillis());
        return backOff;
    }

    static BackOff unknownBackOff(Settings settings) {
        ExponentialBackOff backOff = new ExponentialBackOff(settings.initialInterval().toMillis(), 2.0);
        backOff.setMaxInterval(settings.maxInterval().toMillis());
        backOff.setMaxAttempts(settings.unknownMaxAttempts());
        return backOff;
    }

    private static String rootMessage(Throwable e) {
        Throwable root = FailureClassifier.rootCause(e);
        return root.getClass().getSimpleName() + (root.getMessage() == null ? "" : ": " + root.getMessage());
    }
}
