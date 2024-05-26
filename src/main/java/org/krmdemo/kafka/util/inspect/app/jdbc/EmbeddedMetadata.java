package org.krmdemo.kafka.util.inspect.app.jdbc;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.util.*;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class EmbeddedMetadata {
    private String productName;
    private String productVersion;
    private String driverName;
    private String driverVersion;
    private String jdbcVersion;
    private String schema;
    private String catalog;
    private int transactionIsolation;
    private boolean autoCommit;
    private boolean readonly;
}
