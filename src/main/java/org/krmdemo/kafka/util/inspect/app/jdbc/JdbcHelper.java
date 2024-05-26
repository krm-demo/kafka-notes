package org.krmdemo.kafka.util.inspect.app.jdbc;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import static java.lang.String.format;
import static org.krmdemo.kafka.util.inspect.app.jdbc.JdbcUtils.dumpAsJson;

/**
 * Spring-Component to work with JDBC resources of "kafka-inspect" application
 */
@Slf4j
@Component
public class JdbcHelper {

    @Resource(name = "data-source-embedded-H2") // <-- maybe using spring-profile is better
    DataSource embeddedDataSource;

    /**
     * @return the status of embedded database as {@link EmbeddedMetadata} object
     * @throws SQLException in case of database error
     */
    public EmbeddedMetadata embeddedMetadata() throws SQLException {
        try (Connection conn = embeddedDataSource.getConnection()) {
            EmbeddedMetadata metaData = new EmbeddedMetadata();
            metaData.setAutoCommit(conn.getAutoCommit());
            metaData.setReadonly(conn.isReadOnly());
            metaData.setSchema(conn.getSchema());
            metaData.setSchema(conn.getCatalog());
            DatabaseMetaData connMetaData = conn.getMetaData();
            metaData.setJdbcVersion(format("%d.%d",
                connMetaData.getJDBCMajorVersion(),
                connMetaData.getJDBCMinorVersion()));
            metaData.setProductName(connMetaData.getDatabaseProductName());
            metaData.setProductVersion(connMetaData.getDatabaseProductVersion());
            metaData.setDriverName(connMetaData.getDriverName());
            metaData.setDriverVersion(connMetaData.getDriverVersion());
            metaData.setTransactionIsolation(connMetaData.getDefaultTransactionIsolation());
            return metaData;
        }
    }

    /**
     * Getting the status of embedded database in JSON-format
     *
     * @return {@link EmbeddedMetadata} or {@link JdbcUtils.JdbcError} in JSON-format
     */
    public String dumpEmbeddedMetadata() {
        try {
            return dumpAsJson(embeddedMetadata());
        } catch (SQLException sqlEx) {
            return dumpAsJson(JdbcUtils.JdbcError.valueOf(sqlEx));
        }
    }

    /**
     * Create or re-create if exists the main table for kafka-records in embedded database
     * @return
     */
    public String createTableKafkaRecord() {
        try {
            // TODO: try " RUNSCRIPT FROM 'classpath:/.....' "
            String sql = IOUtils.resourceToString("/jdbc/create-table--kafka-record.sql", Charset.defaultCharset());
            JdbcTemplate jdbcTemplate = new JdbcTemplate(embeddedDataSource);
            jdbcTemplate.execute(sql);
            return "";
        } catch (Exception ex) {
            log.error("could not create a table for kafka-record", ex);
            return ex.getMessage();
        }
    }
}
