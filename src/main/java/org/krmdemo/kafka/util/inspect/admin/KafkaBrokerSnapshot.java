package org.krmdemo.kafka.util.inspect.admin;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.krmdemo.kafka.util.KafkaUtils;
import org.krmdemo.kafka.util.inspect.JsonResult;

import java.time.Instant;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.time.LocalTime.ofInstant;
import static java.util.Arrays.asList;
import static java.util.function.Function.identity;
import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.krmdemo.kafka.util.KafkaUtils.fromTopicPartitionKey;
import static org.krmdemo.kafka.util.StreamUtils.toLinkedMap;
import static org.krmdemo.kafka.util.StreamUtils.toSortedMap;
import static org.krmdemo.kafka.util.inspect.JsonResult.dumpAsJson;

/**
 *
 */
@Data
@JsonInclude(Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class KafkaBrokerSnapshot {

    private final Instant takenAt = Instant.now();
    private final Map<String, Object> adminProps;

    private JsonResult.ClusterInfo clusterInfo = null;
    private final SortedMap<String, KafkaBrokerTopic> topicsMap = new TreeMap<>();

    private List<JsonResult.AnyError> refreshTopicsErrors = null;
    private List<JsonResult.AnyError> refreshOffsetsErrors = null;

    @JsonGetter("takenAtFmt")
    public String takenAtFmt() {
        return "" + ofInstant(takenAt, ZoneId.systemDefault());
    }

    @JsonGetter("bootstrapServersList")
    public List<String> bootstrapServersList() {
        String bootstrapServers = (String)this.adminProps.get(BOOTSTRAP_SERVERS_CONFIG);
        return asList(StringUtils.split(bootstrapServers, ",;"));
    }

    /**
     * @return the total count of all kafka-records in all kafka-partitions in the whole kafka-cluster
     */
    @JsonGetter("clusterSize")
    public long clusterSize() {
        return topicsMap.values().stream()
            .mapToLong(KafkaBrokerTopic::topicSize)
            .sum();
    }

    public Stream<TopicPartition> topicPartitionStream() {
        return topicsMap.values().stream().flatMap(KafkaBrokerTopic::topicPartitionStream);
    }

    public Map<String, JsonResult.PartitionInfo> partitionsMap() {
        return topicPartitionStream().collect(toSortedMap(KafkaUtils::tpKey, this::partitionInfo));
    }

    public Map<TopicPartition, OffsetSpec> topicPartitionSpecMap(OffsetSpec offsetSpec) {
        return topicPartitionStream().collect(toLinkedMap(identity(), tp -> offsetSpec));
    }

    public KafkaBrokerTopic kafkaBrokerTopic(String topicName) {
        return topicsMap.get(topicName);
    }

    public JsonResult.PartitionInfo partitionInfo(String tpKey) {
        return partitionInfo(fromTopicPartitionKey(tpKey));
    }

    public JsonResult.PartitionInfo partitionInfo(TopicPartition tp) {
        return kafkaBrokerTopic(tp.topic()).partitionInfo(tp.partition());
    }

    /**
     * @return the whole dump of this snapshot as JSON
     */
    public String dump() {
        return dumpAsJson(this);
    }

    /**
     * @return brief info about this snapshot (NOT the whole dump)
     */
    @Override
    public String toString() {
        return format("kafka-snapshot{ %s - at %s }", bootstrapServersList(), takenAtFmt());
    }
}
