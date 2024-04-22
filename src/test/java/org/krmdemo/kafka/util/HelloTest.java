package org.krmdemo.kafka.util;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.time.ZonedDateTime;

@Slf4j
class HelloTest {

    @Test
    void testHello() {
        System.out.println("Hello from " + getClass().getSimpleName());
        log.warn("this is a warning !!!");
        log.warn("info about logger-class: " + log.getClass());
        log.info("some debugging log-message with params: '{}' and '{}'",
            "la-la-la", ZonedDateTime.now());
    }
}
