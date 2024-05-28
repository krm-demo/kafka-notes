package org.krmdemo.kafka.util.inspect;

import com.fasterxml.jackson.core.type.TypeReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.KafkaFuture;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.krmdemo.kafka.util.inspect.JsonResult.dumpAsJson;

@Slf4j
public class KafkaFutureErrors {

    private static ThreadLocal<List<KafkaGetError<?>>> KAFKA_FUTURE_ERRORS = ThreadLocal.withInitial(LinkedList::new);

    public static class KafkaGetError<T> extends JsonResult.AnyError {
        final String typeName;
        private KafkaGetError(Exception ex) {
            super(ex);
            TypeReference<T> typeReference = new TypeReference<T>() {};
            this.typeName = typeReference.getType().getTypeName();
        }
    }

    public static void appendError(KafkaGetError<?> kafkaGetError) {
        KAFKA_FUTURE_ERRORS.get().add(kafkaGetError);
    }

    public static List<KafkaGetError<?>> copyLastErrors() {
        return new ArrayList<>(KAFKA_FUTURE_ERRORS.get());
    }

    public static void clear() {
        clear(true);
    }

    public static void clear(boolean warnIfNotEmpty) {
        List<KafkaGetError<?>> clearedErrors = copyLastErrors();
        KAFKA_FUTURE_ERRORS.get().clear();
        if (KAFKA_FUTURE_ERRORS.get().isEmpty()) {
            log.warn("KAFKA_FUTURE_ERRORS are cleared, but 'lastErrors' was not invoked before: {}",
                dumpAsJson(clearedErrors));
        }
    }

    public static List<KafkaGetError<?>> lastErrors() {
        List<KafkaGetError<?>> clearedErrors = copyLastErrors();
        KAFKA_FUTURE_ERRORS.get().clear();
        return clearedErrors;
    }

    public static <T> Optional<T> kfGet(KafkaFuture<T> kf) {
        try {
            return Optional.of(kf.get());
        } catch (InterruptedException|ExecutionException ex) {
            appendError(new KafkaGetError<T>(ex));
            return Optional.empty();
        }
    }
}
