package org.krmdemo.kafka.util.inspect.admin;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import lombok.Data;

import java.time.Instant;
import java.util.*;

/**
 *
 */
@Data
@JsonInclude(Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class KafkaBrokerSnapshot {

    private final Instant takenAt = Instant.now();
    private final Map<String, Object> adminProps;

}
