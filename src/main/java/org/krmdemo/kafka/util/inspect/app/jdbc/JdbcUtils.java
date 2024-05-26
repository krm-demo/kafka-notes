package org.krmdemo.kafka.util.inspect.app.jdbc;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.List;

/**
 * Utility class to work with JDBC
 */
@Slf4j
public class JdbcUtils {

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class JsonError {
        private String message;
        private String originalMessage;
        private String location;
        private List<String> stackTrace;
        public static JsonError valueOf(JsonProcessingException jsonEx) {
            JsonError err = new JsonError();
            err.setMessage(jsonEx.getMessage());
            err.setOriginalMessage(jsonEx.getOriginalMessage());
            err.setLocation(jsonEx.getLocation().toString());
            err.setStackTrace(ExceptionUtils.getRootCauseStackTraceList(jsonEx));
            return err;
        }
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class JdbcError {
        private int errorCode;
        private String sqlState;
        private String message;
        private List<String> stackTrace;
        public static JdbcError valueOf(SQLException sqlEx) {
            JdbcError err = new JdbcError();
            err.setErrorCode(sqlEx.getErrorCode());
            err.setSqlState(sqlEx.getSQLState());
            err.setMessage(sqlEx.getMessage());
            err.setStackTrace(ExceptionUtils.getRootCauseStackTraceList(sqlEx));
            return err;
        }
    }

    public static String dumpAsJson(Object value) {
        ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .enable(SerializationFeature.INDENT_OUTPUT)
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException jsonEx) {
            log.error("could not dump value as JSON", jsonEx);
            return dumpAsJson(JsonError.valueOf(jsonEx));
        }
    }
}
