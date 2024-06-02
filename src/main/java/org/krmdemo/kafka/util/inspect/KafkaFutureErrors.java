package org.krmdemo.kafka.util.inspect;

import com.fasterxml.jackson.core.type.TypeReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.KafkaFuture;
import org.krmdemo.kafka.util.inspect.JsonResult.AnyError;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.krmdemo.kafka.util.inspect.JsonResult.dumpAsJson;
import static org.krmdemo.kafka.util.inspect.JsonResult.errorFrom;

@Slf4j
public class KafkaFutureErrors {

    private static ThreadLocal<List<AnyError>> KAFKA_FUTURE_ERRORS = ThreadLocal.withInitial(LinkedList::new);

    /**
     * Check if some errors happened since application or unit-test start
     * or since the last invocation of {@link #}
     * @return <code>true</code> if thread-local storage of errors is not empty (otherwise - <code>false</code>)
     */
    public static boolean hasErrors() {
        return !KAFKA_FUTURE_ERRORS.get().isEmpty();
    }

    /**
     * Appends a passed error to the {@link #KAFKA_FUTURE_ERRORS thread-local storage}
     * @param error an exception-wrapper for further JSON-dump
     */
    public static void appendAnyError(AnyError error) {
        KAFKA_FUTURE_ERRORS.get().add(error);
    }

    /**
     * Appends a passed exception to the {@link #KAFKA_FUTURE_ERRORS thread-local storage}
     * @param ex an exception that occurs in methods like {@link #}
     */
    public static void appendException(Exception ex) {
        appendAnyError(errorFrom(ex));
    }

    /**
     * Take a copy of last errors without deleting them
     * @return a copy of last errors from {@link #KAFKA_FUTURE_ERRORS thread-local storage}
     */
    public static List<AnyError> copyLastErrors() {
        return new ArrayList<>(KAFKA_FUTURE_ERRORS.get());
    }

    /**
     * Clear the {@link #KAFKA_FUTURE_ERRORS thread-local storage}
     */
    public static void clear() {
        clear(true);
    }

    public static void clear(boolean warnIfNotEmpty) {
        List<AnyError> clearedErrors = copyLastErrors();
        KAFKA_FUTURE_ERRORS.get().clear();
        if (hasErrors() && warnIfNotEmpty) {
            log.warn("KAFKA_FUTURE_ERRORS are cleared, but 'lastErrors' was not invoked before: {}",
                dumpAsJson(clearedErrors));
        }
    }

    /**
     * Getting the last errors from {@link #KAFKA_FUTURE_ERRORS thread-local storage} and then clear it,
     * so the subsequent invocation to this method will always return an empty list.
     *
     * @return list of errors since the last invocation of this method (or since the start of application or unit-test)
     */
    public static List<AnyError> lastErrors() {
        List<AnyError> clearedErrors = copyLastErrors();
        clear(false);
        return clearedErrors;
    }

    /**
     * Getting the value from {@link KafkaFuture} as java-optional. In case when {@link InterruptedException}
     * or {@link ExecutionException} was caught - an {@link Optional#empty()} will be returned and
     * exception will be stored in {@link #KAFKA_FUTURE_ERRORS thread-local storage}
     * and will be available later with method {@link KafkaFutureErrors#lastErrors()}.
     *
     * @param kf a {@link KafkaFuture} object whose {@link KafkaFuture#get get() method} is going to be invoked
     * @return a java-optional over the result of {@link KafkaFuture#get get() method}
     * @param <T> the real type of intercepting {@link KafkaFuture}
     */
    public static <T> Optional<T> kfGet(KafkaFuture<T> kf) {
        try {
            return Optional.of(kf.get());
        } catch (InterruptedException|ExecutionException ex) {
            appendException(ex);
            return Optional.empty();
        }
    }
}
