package org.krmdemo.kafka.util.inspect.admin;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.krmdemo.kafka.util.KafkaUtils;
import org.krmdemo.kafka.util.inspect.JsonResult.OffsetRange;
import org.krmdemo.kafka.util.inspect.JsonResult.PartitionInfo;
import org.krmdemo.kafka.util.inspect.JsonResult.TopicInfo;

import java.util.*;
import java.util.stream.Stream;

import static org.krmdemo.kafka.util.StreamUtils.toSortedMap;

/**
 * This class represents the snapshot of a given kafka-topic,
 * which is the part of the whole {@link KafkaBrokerSnapshot}
 * <p/>
 * The most important member {@link #partitionsMap} has <b>topic-partition-key</b> as a key and {@link PartitionInfo} as a value
 */
@Data
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class KafkaBrokerTopic {

    private final TopicInfo topicInfo;
    private final List<Integer> partitionsValues;
    private final Map<String, PartitionInfo> partitionsMap = new TreeMap<>();

    KafkaBrokerTopic(TopicDescription td) {
        this.topicInfo = new TopicInfo(td);
        this.partitionsValues = td.partitions().stream().map(TopicPartitionInfo::partition).toList();
        td.partitions().forEach(tpi -> this.partitionsMap.put(tpKey(tpi.partition()), new PartitionInfo(tpi)));
    }

    public String topicName() {
        return topicInfo.getTopicName();
    }

    public TopicPartition tp(int partitionNum) {
        return KafkaUtils.tp(topicName(), partitionNum);
    }

    public Stream<TopicPartition> topicPartitionStream() {
        return partitionsValues.stream().map(this::tp);
    }

    public PartitionInfo partitionInfo(int partitionNum) {
        return partitionsMap.get(tpKey(partitionNum));
    }

    public String tpKey(int partitionNum) {
        return KafkaUtils.tpKey(topicName(), partitionNum);
    }

    public Map<String, OffsetRange> tpKeyToRange() {
        return partitionsMap.entrySet().stream()
            .filter(e -> e.getValue().hasRange())
            .collect(toSortedMap(Map.Entry::getKey, e -> e.getValue().getRange()));
    }

    /**
     * @return the total count of all kafka-records in all kafka-partitions within this kafka-topic
     */
    @JsonGetter("topicSize")
    public long topicSize() {
        return partitionsMap.values().stream()
            .filter(PartitionInfo::hasRange)
            .map(PartitionInfo::getRange)
            .mapToLong(OffsetRange::count)
            .sum();
    }
}
