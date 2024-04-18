package org.krmdemo.app.hello;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
class HelloAppTest {

    @Test
    void testMain() {
        System.out.println("Hello from " + getClass().getSimpleName());
        log.info("la-la-la: " + log.getClass());
    }
}
