package org.krmdemo.kafka.util.inspect;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclOperation;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.krmdemo.kafka.util.StreamUtils.toSortedMap;
import static org.krmdemo.kafka.util.inspect.KafkaFutureErrors.kfGet;

/**
 * Helper classes and utility-methods to construct inspecting objects and dump them in a JSON-format.
 */
@Slf4j
public class JsonResult {

    @Getter @ToString
    @JsonInclude(Include.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class AnyError {
        private final String exceptionClass;
        private final String message;
        private final List<String> stackTrace;
        public AnyError(@NonNull Exception ex) {
            this.exceptionClass = ex.getClass().getName();
            this.message = ex.getMessage();
            this.stackTrace = ExceptionUtils.getRootCauseStackTraceList(ex);
        }
    }

    @Getter @ToString
    @JsonInclude(Include.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class JsonError extends AnyError {
        private final String originalMessage;
        private final String location;
        public JsonError(JsonProcessingException jsonEx) {
            super(jsonEx);
            this.originalMessage = jsonEx.getOriginalMessage();
            this.location = jsonEx.getLocation() == null ? "" : jsonEx.getLocation().toString();
        }
    }

    public static AnyError errorFrom(Exception ex) { return new AnyError(ex); }
    public static JsonError errorFrom(JsonProcessingException jsonEx) { return new JsonError(jsonEx); }

    @Getter @ToString
    @JsonInclude(Include.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class NodeInfo {
        private final String nodeId;
        private final String nodeStr;
        public NodeInfo(@NonNull Node node) {
            this.nodeId = node.idString();
            this.nodeStr = node.toString();
        }
    }

    @Getter @ToString
    @JsonInclude(Include.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ClusterInfo {
        private final String clusterId;
        private final List<NodeInfo> nodes;
        private final NodeInfo controller;
        private final Set<AclOperation> authorizedOperations;
        private final List<AnyError> errors;
        public ClusterInfo(DescribeClusterResult dcr) {
            KafkaFutureErrors.clear();
            this.clusterId = kfGet(dcr.clusterId()).orElse("");
            this.nodes = kfGet(dcr.nodes()).orElse(emptyList()).stream().map(NodeInfo::new).toList();
            this.controller = kfGet(dcr.controller()).map(NodeInfo::new).orElse(null);
            this.authorizedOperations = kfGet(dcr.authorizedOperations()).orElse(emptySet());
            this.errors = KafkaFutureErrors.lastErrors();
        }
    }

    @Data
    @JsonInclude(Include.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class OffsetRange {
        private Long offsetMin;
        private Long offsetMax;
//        private Long timestampMin;
//        private Long timestampMax;
        @JsonGetter("count") public long count() {
            return offsetMin == null || offsetMax == null ? 0 : (offsetMax - offsetMin);
        }
//        @JsonGetter("duration") public long duration() {
//            return timestampMin == null || timestampMax == null ? 0 : (timestampMax - timestampMin);
//        }
        @JsonGetter("interval") public String intervalOffsets() {
            return count() == 0 ? "<empty-offset-range>" : format("( %d ; %d )", offsetMin, offsetMax);
        }
        public OffsetRange(@NonNull ListOffsetsResultInfo offsetsEarliest,
                           @NonNull ListOffsetsResultInfo offsetsLatest) {
            this.offsetMin = offsetsEarliest.offset();
            this.offsetMax = offsetsLatest.offset();
        }
    }

    @Getter @ToString
    @JsonInclude(Include.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class PartitionInfo {
        private final int partitionNum;
        private final NodeInfo leaderNode;
        private final List<NodeInfo> replicas;
        private final List<NodeInfo> replicasInSync;
        private final OffsetRange range = null;
        public PartitionInfo(TopicPartitionInfo tpi) {
            this.partitionNum = tpi.partition();
            this.leaderNode = new NodeInfo(tpi.leader());
            this.replicas = tpi.replicas().stream().map(NodeInfo::new).toList();
            this.replicasInSync = tpi.isr().stream().map(NodeInfo::new).toList();
        }
        public boolean hasRange() {
            return range != null;
        }
    }

    @Getter @ToString
    @JsonInclude(Include.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TopicInfo {
        private final String topicId;
        private final String topicName;
        private final boolean internal;
        private final Set<AclOperation> authorizedOperations;
        private final Map<Integer, PartitionInfo> partitions;
        public TopicInfo(TopicDescription td) {
            this.topicId = td.topicId().toString();
            this.topicName = td.name();
            this.internal = td.isInternal();
            this.authorizedOperations = td.authorizedOperations();
            this.partitions = td.partitions().stream()
                .collect(toSortedMap(
                    TopicPartitionInfo::partition,
                    PartitionInfo::new
                ));
        }
    }

    private static final ObjectMapper OBJECT_MAPPER_DUMP =
        new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .enable(SerializationFeature.INDENT_OUTPUT)
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);

    public static String dumpAsJson(Object value) {
        try {
            return OBJECT_MAPPER_DUMP.writeValueAsString(value);
        } catch (JsonProcessingException jsonEx) {
            log.error("could not dump the value as JSON", jsonEx);
            return dumpAsJson(errorFrom(jsonEx));
        }
    }

    private static final ObjectMapper OBJECT_MAPPER_PRETTY_PRINT = objectMapperPrettyPrint();
    private static ObjectMapper objectMapperPrettyPrint() {
        ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .enable(SerializationFeature.INDENT_OUTPUT)
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
        objectMapper.configOverride(Map.class).setInclude(
            JsonInclude.Value.construct(Include.NON_EMPTY, Include.NON_EMPTY));
        return objectMapper;
    }

    public static String prettyPrintJson(String jsonToPrettify) {
        if (isBlank(jsonToPrettify)) {
            log.warn("attempt to pretty-print the blank string - return the empty string");
            return "";
        }
        try {
            Object objFromJson = OBJECT_MAPPER_DUMP.readValue(jsonToPrettify, Object.class);
            return OBJECT_MAPPER_PRETTY_PRINT.writeValueAsString(objFromJson);
        } catch (JsonProcessingException jsonEx) {
            log.error("could not pretty-print JSON-string - '" + jsonToPrettify + "''", jsonEx);
            return jsonToPrettify;
        }
    }

    private JsonResult() {
        // prohibit the creation of utility-class instance
        throw new UnsupportedOperationException("Cannot instantiate utility-class " + getClass().getName());
    }
}
