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
    public DataSource embeddedDataSource() {
        DataSourceBuilder<?> dataSourceBuilder = DataSourceBuilder.create();
        dataSourceBuilder.driverClassName(DRIVER_H2);
        dataSourceBuilder.url("jdbc:h2:mem:topics");
        dataSourceBuilder.username("SA");
        dataSourceBuilder.password("");
        DataSource dataSource = dataSourceBuilder.build();
        log.info("DataSource for embedded H2 was created of type " + dataSource.getClass());
        return dataSource;
    }
}
