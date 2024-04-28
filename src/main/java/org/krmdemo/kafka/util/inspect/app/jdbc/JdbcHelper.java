package org.krmdemo.kafka.util.inspect.app.jdbc;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.Charset;

@Slf4j
@Component
public class JdbcHelper {

    @Resource(name = "jdbc-template-embedded-H2") // <-- maybe using spring-profile is better
    JdbcTemplate jdbcTemplate;

    public String createTableKafkaRecord() {
        try {
            // TODO: try " RUNSCRIPT FROM 'classpath:/.....' "
            String sql = IOUtils.resourceToString("/jdbc/create-table--kafka-record.sql", Charset.defaultCharset());
            jdbcTemplate.execute(sql);
            return "";
        } catch (Exception ex) {
            log.error("could not create a table for kafka-record", ex);
            return ex.getMessage();
        }
    }
}
