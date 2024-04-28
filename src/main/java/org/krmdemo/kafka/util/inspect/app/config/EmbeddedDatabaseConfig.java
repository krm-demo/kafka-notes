package org.krmdemo.kafka.util.inspect.app.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;


@Slf4j
@Configuration
public class EmbeddedDatabaseConfig {

    final String DRIVER_H2 = "org.h2.Driver";

    @Bean("data-source-embedded-H2")
    public DataSource getDataSource() {
        DataSourceBuilder<?> dataSourceBuilder = DataSourceBuilder.create();
        dataSourceBuilder.driverClassName(DRIVER_H2);
        dataSourceBuilder.url("jdbc:h2:mem:topics");
        dataSourceBuilder.username("SA");
        dataSourceBuilder.password("");
        DataSource dataSource = dataSourceBuilder.build();
        log.info("DataSource for embedded H2 was created of type " + dataSource.getClass());
        return dataSource;
    }

    @Bean("jdbc-template-embedded-H2")
    public JdbcTemplate jdbcTemplate(DataSource dataSource) {
        // TODO: think about using connection-pool
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        log.info("JdbcTemplate is ready to use");
        return jdbcTemplate;
    }
}
