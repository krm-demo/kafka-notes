package org.krmdemo.kafka.util.inspect.app;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.krmdemo.kafka.util.inspect.app.jdbc.JdbcHelper;
import org.krmdemo.kafka.util.inspect.app.jdbc.JdbcUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.EventListener;
import org.springframework.lang.NonNull;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.servlet.handler.SimpleUrlHandlerMapping;

import javax.sql.DataSource;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.lang.System.identityHashCode;
import static org.krmdemo.kafka.util.StreamUtils.stream;
import static org.krmdemo.kafka.util.SpringWebUtils.getHandlerMapping;

@Slf4j
@Profile("!dev")
//@EnableWebMvc
@SpringBootApplication
public class KafkaInspectApp implements WebMvcConfigurer {

    @Value("${spring.application.name}")
    private String springAppName;

    @Override
    public void addResourceHandlers(@NonNull ResourceHandlerRegistry registry) {
        registry.addResourceHandler("/js/**").addResourceLocations("classpath:/static/js/");
        registry.addResourceHandler("/css/**").addResourceLocations("classpath:/static/css/");
        registry.addResourceHandler("/images/**").addResourceLocations("classpath:/static/images/");
        registry.addResourceHandler("/**").addResourceLocations("classpath:/static/"); // <-- ???

        SimpleUrlHandlerMapping mapping = getHandlerMapping(registry);
        if (mapping == null) {
            log.warn("getHandlerMapping(registry) returns null");
            return;
        }
        String mappingDump = mapping.getUrlMap().entrySet().stream()
            .map(e -> format("- '%s' --> %s(0x%X)", e.getKey(),
                e.getValue().getClass().getSimpleName(), identityHashCode(e.getValue())))
            .collect(Collectors.joining("\n", "\n... ResourceHandlerRegistry contains: ...\n", "\n"));
        log.info(mappingDump);
    }

    @Resource(name = "data-source-embedded-H2")
    private DataSource dsH2;

    @Autowired
    private JdbcHelper jdbcHelper;

    @EventListener(ApplicationReadyEvent.class)
    void onApplicationReady(final ApplicationReadyEvent event) throws Exception {
        log.info("application '{}' is ready", springAppName);
        log.info("embedded H2 metadata: " + JdbcUtils.dumpMetadata(dsH2));
        if (StringUtils.isBlank(jdbcHelper.createTableKafkaRecord())) {
            log.info("a table for kafka-record was created");
        }
    }

    /**
     * JVM entry-point
     * @param args command-line arguments
     */
    public static void main(String[] args) {
        SpringApplication springApplication = new SpringApplication(KafkaInspectApp.class);
        springApplication.run(args);
    }

}
