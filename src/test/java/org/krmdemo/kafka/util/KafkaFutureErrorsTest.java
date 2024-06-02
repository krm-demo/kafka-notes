package org.krmdemo.kafka.util;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;
import org.krmdemo.kafka.util.inspect.JsonResult.AnyError;
import org.krmdemo.kafka.util.inspect.KafkaFutureErrors;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.apache.kafka.common.KafkaFuture.completedFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.krmdemo.kafka.util.inspect.JsonResult.dumpAsJson;
import static org.krmdemo.kafka.util.inspect.KafkaFutureErrors.kfGet;

/**
 * A unit-test for verifying the usage of utility-method {@link KafkaFutureErrors#kfGet(KafkaFuture)}.
 */
public class KafkaFutureErrorsTest {

    @Test
    void testIgnoreSingleError() throws ExecutionException, InterruptedException {
        assertThat(futureOfList().get()).hasSize(3);
        assertThatExceptionOfType(ExecutionException.class).isThrownBy(() -> {
            // access to KafkaFuture with predefined error on 'get':
            futureOfListWithError("some error, which is ignored and thrown out").get();
        });
    }

    @Test
    void testHandleSingleError() {
        // 'no-error' access to single KafkaFuture with 'kfGet' (no checked exception to handle)
        assertThat(kfGet(futureOfList()).orElse(emptyList())).hasSize(3);
        // when KafkaFuture has a predefined error on 'get' - the invocation of 'kfGet' returns 'Optional.empty()'
        assertThat(kfGet(futureOfListWithError("some error that will be handled later"))).isEmpty();
        assertThat(KafkaFutureErrors.hasErrors()).isTrue();
        List<AnyError> lastErrors = KafkaFutureErrors.lastErrors();  // <-- the storage is cleared here
        assertThat(lastErrors).hasSize(1);
        assertThat(dumpAsJson(lastErrors)).contains("will be handled later");
        // subsequent invocation of 'lastErrors' must return the empty list and 'hasErrors' must return 'false'
        assertThat(KafkaFutureErrors.hasErrors()).isFalse();
        assertThat(KafkaFutureErrors.lastErrors()).isEmpty();
    }

    private KafkaFuture<List<String>> futureOfList() {
        return completedFuture(asList("one", "two", "three"));
    }

    private KafkaFuture<List<String>> futureOfListWithError(String errMsg) {
        return completedExceptionally(errMsg);
    }

    private static <T> KafkaFuture<T> completedExceptionally(String errMsg) {
        KafkaFutureImpl<T> kf = new KafkaFutureImpl<>();
        kf.completeExceptionally(new IllegalStateException(errMsg));
        return kf;
    }
}
