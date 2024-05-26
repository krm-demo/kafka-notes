package org.krmdemo.kafka.util;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.krmdemo.kafka.util.inspect.app.config.EmbeddedDatabaseConfig;
import org.krmdemo.kafka.util.inspect.app.jdbc.JdbcHelper;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@SpringBootTest(
    properties = {
        "spring.main.banner-mode = off",
    },
    classes = {JdbcHelper.class, EmbeddedDatabaseConfig.class}
)
public class JdbcTest {

    @Resource
    JdbcHelper jdbcHelper;

    @Test
    void testJdbcHelper() {
        log.error("this is a test error from spring-test"); // , new RuntimeException("test exception"));
        log.warn("this is a warning from spring-test !!!");
        log.info("info about logger-class from spring-test: " + log.getClass());
        log.debug("some debugging log-message from spring-test with params: '{}' and '{}'",
            "la-la-la", ZonedDateTime.now());
        assertThat(jdbcHelper).isNotNull();
    }
}
